mod pumpfun_detector;

use {
    clap::{App, Arg},
    pumpfun_detector::{
        decode_entries, detect_tracked_buys, parse_watchlist_command, BoundedFecState, FecShreds,
        SignatureDedupe, Watchlist, MAX_DATA_SHREDS_PER_SLOT, MAX_SEEN_SHREDS_PER_SLOT,
        MAX_SHREDS_PER_FEC_SET, SLOT_WINDOW,
    },
    solana_gossip::{
        cluster_info::ClusterInfo,
        contact_info::{ContactInfo, Protocol},
        gossip_service::GossipService,
    },
    solana_keypair::{read_keypair_file, Keypair},
    solana_ledger::shred::{self, ReedSolomonCache, Shred, ShredId, Shredder},
    solana_net_utils::{get_cluster_shred_version, get_public_ip_addr, parse_host_port},
    solana_packet::PACKET_DATA_SIZE,
    solana_signer::Signer,
    solana_streamer::socket::SocketAddrSpace,
    solana_time_utils::timestamp,
    std::{
        collections::{BTreeMap, HashSet},
        error::Error,
        io,
        net::{IpAddr, SocketAddr, TcpListener, UdpSocket},
        path::Path,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread,
        time::Duration,
    },
};

#[cfg(unix)]
use std::os::unix::{fs::FileTypeExt, net::UnixDatagram};

const DEFAULT_EVENT_SOCKET: &str = "/tmp/pumpfun.sock";
const DEFAULT_CONTROL_SOCKET: &str = "/tmp/turbine-control.sock";

#[derive(Default)]
struct SlotAssembly {
    next_index: u32,
    data: BTreeMap<u32, Shred>,
}

impl SlotAssembly {
    fn insert(&mut self, shred: Shred) -> Vec<Vec<solana_entry::entry::Entry>> {
        if shred.is_data() && shred.index() >= self.next_index {
            self.data.entry(shred.index()).or_insert(shred);
        }
        if self.data.len() > MAX_DATA_SHREDS_PER_SLOT {
            self.data.clear();
            return Vec::new();
        }
        let mut completed = Vec::new();
        loop {
            let mut end = self.next_index;
            let complete_end = loop {
                let Some(shred) = self.data.get(&end) else {
                    break None;
                };
                if shred.data_complete() || shred.last_in_slot() {
                    break Some(end);
                }
                end = end.saturating_add(1);
            };
            let Some(end) = complete_end else {
                break;
            };
            let shreds: Vec<_> = (self.next_index..=end)
                .filter_map(|index| self.data.remove(&index))
                .collect();
            self.next_index = end.saturating_add(1);
            completed.push(decode_entries(shreds));
        }
        completed
    }
}

#[cfg(unix)]
fn spawn_control_socket(
    path: &str,
    watchlist: Arc<Watchlist>,
    exit: Arc<AtomicBool>,
) -> Result<thread::JoinHandle<()>, io::Error> {
    if Path::new(path).exists() {
        if !std::fs::symlink_metadata(path)?.file_type().is_socket() {
            return Err(io_error(format!(
                "refusing to replace non-socket control path {path}"
            )));
        }
        std::fs::remove_file(path)?;
    }
    let socket = UnixDatagram::bind(path)?;
    socket.set_read_timeout(Some(Duration::from_millis(250)))?;
    let path = path.to_string();
    thread::Builder::new()
        .name("turbine-watchlist".to_string())
        .spawn(move || {
            let mut buffer = [0u8; 64 * 1024];
            while !exit.load(Ordering::Relaxed) {
                match socket.recv(&mut buffer) {
                    Ok(size) => match parse_watchlist_command(&buffer[..size]) {
                        Ok(command) => {
                            watchlist.apply(command);
                        }
                        Err(err) => log::warn!("discarding invalid watchlist command: {err}"),
                    },
                    Err(err)
                        if matches!(
                            err.kind(),
                            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                        ) => {}
                    Err(err) => {
                        log::warn!("watchlist control socket error: {err}");
                        thread::sleep(Duration::from_millis(25));
                    }
                }
            }
            drop(socket);
            let _ = std::fs::remove_file(path);
        })
}

#[cfg(unix)]
fn create_event_socket() -> Result<UnixDatagram, io::Error> {
    let socket = UnixDatagram::unbound()?;
    socket.set_nonblocking(true)?;
    Ok(socket)
}

fn recover_fec_set(shreds: &FecShreds, cache: &ReedSolomonCache) -> Vec<Shred> {
    let values: Vec<_> = shreds.values().cloned().collect();
    match shred::recover(values.clone(), cache) {
        Ok(recovered) => recovered.filter_map(Result::ok).collect(),
        Err(_) => Shredder::try_recovery(values, cache).unwrap_or_default(),
    }
}

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
        .arg(
            Arg::with_name("event-socket")
                .long("event-socket")
                .takes_value(true)
                .default_value(DEFAULT_EVENT_SOCKET)
                .help("Bot-owned Unix datagram socket that receives tracked-buy events"),
        )
        .arg(
            Arg::with_name("control-socket")
                .long("control-socket")
                .takes_value(true)
                .default_value(DEFAULT_CONTROL_SOCKET)
                .help("Client-owned Unix datagram socket for ADD/REMOVE/REPLACE commands"),
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
    let event_socket_path = required_value(&matches, "event-socket")?;
    let control_socket_path = required_value(&matches, "control-socket")?;

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
    let gossip_echo_listener =
        TcpListener::bind(SocketAddr::new(bind_ip, gossip_port)).map_err(|err| {
            io_error(format!(
                "failed to bind gossip IP-echo TCP socket on {bind_ip}:{gossip_port}: {err}"
            ))
        })?;
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

    let watchlist = Arc::new(Watchlist::default());
    #[cfg(unix)]
    let control_socket =
        spawn_control_socket(control_socket_path, watchlist.clone(), exit.clone())?;
    #[cfg(unix)]
    let event_socket = create_event_socket()?;
    #[cfg(not(unix))]
    return Err(io_error("solana-turbine-client Unix IPC requires a Unix host").into());

    tvu_socket.set_read_timeout(Some(Duration::from_millis(250)))?;

    eprintln!(
        "turbine-client identity={} entrypoint={} shred_version={} gossip={} tvu={} tpu={} event_socket={} control_socket={} storage=none",
        identity.pubkey(),
        entrypoint,
        shred_version,
        gossip_addr,
        tvu_addr,
        tpu_addr,
        event_socket_path,
        control_socket_path,
    );

    let mut buffer = [0u8; PACKET_DATA_SIZE];
    let mut seen: BTreeMap<u64, HashSet<ShredId>> = BTreeMap::new();
    let mut fec_state = BoundedFecState::<FecShreds>::default();
    let mut slot_assemblies = BTreeMap::<u64, SlotAssembly>::new();
    let mut transaction_dedupe = SignatureDedupe::new(SLOT_WINDOW);
    let reed_solomon_cache = ReedSolomonCache::default();

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
        let seen_slot = seen.entry(slot).or_default();
        if seen_slot.len() >= MAX_SEEN_SHREDS_PER_SLOT || !seen_slot.insert(id) {
            continue;
        }
        let floor = slot.saturating_sub(SLOT_WINDOW);
        seen.retain(|seen_slot, _| *seen_slot >= floor);
        slot_assemblies.retain(|assembly_slot, _| *assembly_slot >= floor);

        let fec_set_index = shred.fec_set_index();
        let mut fec_set = fec_state
            .slots
            .get_mut(&slot)
            .and_then(|sets| sets.remove(&fec_set_index))
            .unwrap_or_default();
        if fec_set.len() >= MAX_SHREDS_PER_FEC_SET {
            fec_state.insert(slot, fec_set_index, fec_set);
            continue;
        }
        fec_set.insert(id, shred.clone());
        let recovered = recover_fec_set(&fec_set, &reed_solomon_cache);
        for recovered_shred in &recovered {
            fec_set.insert(recovered_shred.id(), recovered_shred.clone());
        }
        fec_state.insert(slot, fec_set_index, fec_set);

        let assembly = slot_assemblies.entry(slot).or_default();
        let mut completed = Vec::new();
        if shred.is_data() {
            completed.extend(assembly.insert(shred));
        }
        for recovered_shred in recovered {
            if recovered_shred.is_data() {
                completed.extend(assembly.insert(recovered_shred));
            }
        }
        for entries in completed {
            for transaction in entries.into_iter().flat_map(|entry| entry.transactions) {
                let Some(signature) = transaction.signatures.first().copied() else {
                    continue;
                };
                if !transaction_dedupe.insert(slot, signature) {
                    continue;
                }
                for buy in detect_tracked_buys(slot, &transaction, &watchlist) {
                    let Ok(payload) = buy.encode() else {
                        continue;
                    };
                    #[cfg(unix)]
                    match event_socket.send_to(&payload, event_socket_path) {
                        Ok(_) => {}
                        Err(err)
                            if matches!(
                                err.kind(),
                                io::ErrorKind::WouldBlock
                                    | io::ErrorKind::NotFound
                                    | io::ErrorKind::ConnectionRefused
                            ) => {}
                        Err(err) => log::debug!("tracked-buy UDS send failed: {err}"),
                    }
                }
            }
        }
    }

    exit.store(true, Ordering::Relaxed);
    let _ = tpu_drain.join();
    #[cfg(unix)]
    let _ = control_socket.join();
    gossip_service
        .join()
        .map_err(|_| io_error("gossip service thread panicked"))?;

    Ok(())
}
