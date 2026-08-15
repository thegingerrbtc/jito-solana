use {
    clap::{App, Arg},
    solana_gossip::{
        cluster_info::ClusterInfo,
        contact_info::{ContactInfo, Protocol},
        gossip_service::GossipService,
    },
    solana_keypair::{read_keypair_file, Keypair},
    solana_ledger::shred::{Shred, ShredId},
    solana_net_utils::{get_cluster_shred_version, get_public_ip_addr, parse_host_port},
    solana_packet::PACKET_DATA_SIZE,
    solana_signer::Signer,
    solana_streamer::socket::SocketAddrSpace,
    solana_time_utils::timestamp,
    std::{
        collections::{BTreeMap, HashSet},
        error::Error,
        io::{self, BufWriter, Write},
        net::{IpAddr, SocketAddr, TcpListener, UdpSocket},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread,
        time::Duration,
    },
};

const SEEN_SLOT_WINDOW: u64 = 8;

fn io_error(message: impl Into<String>) -> io::Error {
    io::Error::other(message.into())
}

fn required_value<'a>(matches: &'a clap::ArgMatches<'a>, name: &str) -> Result<&'a str, io::Error> {
    matches
        .value_of(name)
        .ok_or_else(|| io_error(format!("missing --{name}")))
}

fn parse_port(matches: &clap::ArgMatches<'_>, name: &str) -> Result<u16, Box<dyn Error>> {
    Ok(required_value(matches, name)?.parse::<u16>()?)
}

fn bind_udp(bind_ip: IpAddr, port: u16, label: &str) -> Result<UdpSocket, io::Error> {
    UdpSocket::bind(SocketAddr::new(bind_ip, port)).map_err(|err| {
        io_error(format!(
            "failed to bind {label} UDP socket on {bind_ip}:{port}: {err}"
        ))
    })
}

fn spawn_tpu_drain(socket: UdpSocket, exit: Arc<AtomicBool>) -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name("turbine-tpu-drain".to_string())
        .spawn(move || {
            let _ = socket.set_read_timeout(Some(Duration::from_millis(250)));
            let mut buffer = [0u8; PACKET_DATA_SIZE];
            while !exit.load(Ordering::Relaxed) {
                match socket.recv_from(&mut buffer) {
                    Ok(_) => {}
                    Err(err)
                        if matches!(
                            err.kind(),
                            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                        ) => {}
                    Err(err) => {
                        log::warn!("TPU drain socket error: {err}");
                        thread::sleep(Duration::from_millis(100));
                    }
                }
            }
        })
        .expect("failed to spawn TPU drain thread")
}

fn main() -> Result<(), Box<dyn Error>> {
    solana_logger::setup();

    let matches = App::new("solana-turbine-client")
        .about("Stateless Solana Gossip/Turbine client that receives and decodes live shreds")
        .arg(
            Arg::with_name("identity")
                .long("identity")
                .takes_value(true)
                .required(true)
                .help("Validator identity keypair JSON file"),
        )
        .arg(
            Arg::with_name("entrypoint")
                .long("entrypoint")
                .takes_value(true)
                .required(true)
                .help("Cluster gossip entrypoint HOST:PORT"),
        )
        .arg(
            Arg::with_name("bind-address")
                .long("bind-address")
                .takes_value(true)
                .default_value("0.0.0.0")
                .help("Local IP address used to bind sockets"),
        )
        .arg(
            Arg::with_name("public-address")
                .long("public-address")
                .takes_value(true)
                .help("Public IP to advertise; inferred from the entrypoint when omitted"),
        )
        .arg(
            Arg::with_name("gossip-port")
                .long("gossip-port")
                .takes_value(true)
                .default_value("8001")
                .help("UDP/TCP gossip port to bind and advertise"),
        )
        .arg(
            Arg::with_name("tvu-port")
                .long("tvu-port")
                .takes_value(true)
                .default_value("8002")
                .help("UDP TVU port to bind and advertise for Turbine shreds"),
        )
        .arg(
            Arg::with_name("tpu-port")
                .long("tpu-port")
                .takes_value(true)
                .default_value("8003")
                .help("UDP TPU port to bind/advertise so the contact record is not classified as a spy"),
        )
        .arg(
            Arg::with_name("shred-version")
                .long("shred-version")
                .takes_value(true)
                .help("Cluster shred version; inferred from the entrypoint when omitted"),
        )
        .get_matches();

    let identity_path = required_value(&matches, "identity")?;
    let identity: Keypair = read_keypair_file(identity_path).map_err(|err| {
        io_error(format!(
            "failed to read identity keypair {identity_path}: {err}"
        ))
    })?;
    let identity = Arc::new(identity);

    let entrypoint = parse_host_port(required_value(&matches, "entrypoint")?)
        .map_err(|err| io_error(format!("invalid entrypoint: {err}")))?;
    let bind_ip = required_value(&matches, "bind-address")?.parse::<IpAddr>()?;
    let gossip_port = parse_port(&matches, "gossip-port")?;
    let tvu_port = parse_port(&matches, "tvu-port")?;
    let tpu_port = parse_port(&matches, "tpu-port")?;

    let shred_version = match matches.value_of("shred-version") {
        Some(value) => value.parse::<u16>()?,
        None => get_cluster_shred_version(&entrypoint)
            .map_err(|err| io_error(format!("failed to discover shred version: {err}")))?,
    };
    let public_ip = match matches.value_of("public-address") {
        Some(value) => value.parse::<IpAddr>()?,
        None => get_public_ip_addr(&entrypoint)
            .map_err(|err| io_error(format!("failed to discover public IP: {err}")))?,
    };

    let gossip_socket = bind_udp(bind_ip, gossip_port, "gossip")?;
    let gossip_echo_listener = TcpListener::bind(SocketAddr::new(bind_ip, gossip_port)).map_err(
        |err| {
            io_error(format!(
                "failed to bind gossip IP-echo TCP socket on {bind_ip}:{gossip_port}: {err}"
            ))
        },
    )?;
    let tvu_socket = bind_udp(bind_ip, tvu_port, "TVU")?;
    let tpu_socket = bind_udp(bind_ip, tpu_port, "TPU")?;

    let gossip_addr = SocketAddr::new(public_ip, gossip_port);
    let tvu_addr = SocketAddr::new(public_ip, tvu_port);
    let tpu_addr = SocketAddr::new(public_ip, tpu_port);

    let mut contact_info = ContactInfo::new(identity.pubkey(), timestamp(), shred_version);
    contact_info.set_gossip(gossip_addr)?;
    contact_info.set_tvu(Protocol::UDP, tvu_addr)?;
    contact_info.set_tpu(tpu_addr)?;

    let cluster_info = Arc::new(ClusterInfo::new(
        contact_info,
        identity.clone(),
        SocketAddrSpace::Unspecified,
    ));
    cluster_info.set_entrypoint(ContactInfo::new_gossip_entry_point(&entrypoint));

    let exit = Arc::new(AtomicBool::new(false));
    {
        let exit = exit.clone();
        ctrlc::set_handler(move || {
            exit.store(true, Ordering::Relaxed);
        })?;
    }

    let _ip_echo_runtime = solana_net_utils::ip_echo_server(
        gossip_echo_listener,
        solana_net_utils::DEFAULT_IP_ECHO_SERVER_THREADS,
        Some(shred_version),
    );
    let gossip_service = GossipService::new(
        &cluster_info,
        None, // No BankForks: this client has no ledger/runtime state.
        gossip_socket,
        None,
        true,
        None,
        exit.clone(),
    );
    let tpu_drain = spawn_tpu_drain(tpu_socket, exit.clone());

    tvu_socket.set_read_timeout(Some(Duration::from_millis(250)))?;

    eprintln!(
        "turbine-client identity={} entrypoint={} shred_version={} gossip={} tvu={} tpu={} storage=none",
        identity.pubkey(),
        entrypoint,
        shred_version,
        gossip_addr,
        tvu_addr,
        tpu_addr,
    );

    let stdout = io::stdout();
    let mut output = BufWriter::new(stdout.lock());
    writeln!(
        output,
        "wallclock_ms\tsource\tslot\tindex\ttype\tfec_set_index"
    )?;
    output.flush()?;

    let mut buffer = [0u8; PACKET_DATA_SIZE];
    let mut seen: BTreeMap<u64, HashSet<ShredId>> = BTreeMap::new();

    while !exit.load(Ordering::Relaxed) {
        let (size, source) = match tvu_socket.recv_from(&mut buffer) {
            Ok(value) => value,
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                continue;
            }
            Err(err) => return Err(err.into()),
        };

        let shred = match Shred::new_from_serialized_shred(buffer[..size].to_vec()) {
            Ok(shred) => shred,
            Err(err) => {
                log::debug!("discarding undecodable TVU datagram from {source}: {err}");
                continue;
            }
        };

        let id = shred.id();
        let slot = id.slot();
        if !seen.entry(slot).or_default().insert(id) {
            continue;
        }
        let floor = slot.saturating_sub(SEEN_SLOT_WINDOW);
        seen.retain(|seen_slot, _| *seen_slot >= floor);

        writeln!(
            output,
            "{}\t{}\t{}\t{}\t{:?}\t{}",
            timestamp(),
            source,
            slot,
            id.index(),
            id.shred_type(),
            shred.fec_set_index(),
        )?;
    }

    output.flush()?;
    exit.store(true, Ordering::Relaxed);
    let _ = tpu_drain.join();
    gossip_service
        .join()
        .map_err(|_| io_error("gossip service thread panicked"))?;

    Ok(())
}
