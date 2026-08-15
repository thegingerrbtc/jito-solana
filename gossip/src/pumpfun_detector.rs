use {
    solana_entry::entry::Entry,
    solana_hash::Hash,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    solana_transaction::versioned::VersionedTransaction,
    std::{
        collections::{BTreeMap, HashMap, HashSet},
        io,
        str::FromStr,
        sync::{OnceLock, RwLock},
        time::{SystemTime, UNIX_EPOCH},
    },
};

pub const PUMPFUN_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
pub const BUY_DISCRIMINATOR: [u8; 8] = [0x66, 0x06, 0x3d, 0x12, 0x01, 0xda, 0xeb, 0xea];
pub const EVENT_MAGIC: [u8; 4] = *b"PFB1";
pub const BUY_TAG: u8 = 0;
pub const MAX_TRACKED_MINTS: usize = 65_536;
pub const MAX_SIGNATURES_PER_SLOT: usize = 65_536;
pub const MAX_SHREDS_PER_FEC_SET: usize = 128;
pub const MAX_SEEN_SHREDS_PER_SLOT: usize = 8_192;
const EVENT_FIXED_LEN: usize = 4 + 1 + 8 + 8 + 64 + 32 * 4 + 8 + 8 + 32 + 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackedBuy {
    pub observed_ns: u64,
    pub slot: u64,
    pub signature: Signature,
    pub mint: Pubkey,
    pub bonding_curve: Pubkey,
    pub associated_bonding_curve: Pubkey,
    pub buyer: Pubkey,
    pub token_amount: u64,
    pub max_sol_cost: u64,
    pub recent_blockhash: Hash,
    pub instruction_data: Vec<u8>,
}

impl TrackedBuy {
    pub fn encode(&self) -> io::Result<Vec<u8>> {
        let instruction_len = u16::try_from(self.instruction_data.len())
            .map_err(|_| io::Error::other("Pump.fun instruction exceeds UDS wire limit"))?;
        let mut out = Vec::with_capacity(EVENT_FIXED_LEN + self.instruction_data.len());
        out.extend_from_slice(&EVENT_MAGIC);
        out.push(BUY_TAG);
        out.extend_from_slice(&self.observed_ns.to_le_bytes());
        out.extend_from_slice(&self.slot.to_le_bytes());
        out.extend_from_slice(self.signature.as_ref());
        out.extend_from_slice(self.mint.as_ref());
        out.extend_from_slice(self.bonding_curve.as_ref());
        out.extend_from_slice(self.associated_bonding_curve.as_ref());
        out.extend_from_slice(self.buyer.as_ref());
        out.extend_from_slice(&self.token_amount.to_le_bytes());
        out.extend_from_slice(&self.max_sol_cost.to_le_bytes());
        out.extend_from_slice(self.recent_blockhash.as_ref());
        out.extend_from_slice(&instruction_len.to_le_bytes());
        out.extend_from_slice(&self.instruction_data);
        Ok(out)
    }
}

#[derive(Debug, Default)]
pub struct Watchlist(RwLock<HashSet<Pubkey>>);

impl Watchlist {
    #[inline]
    pub fn contains(&self, mint: &Pubkey) -> bool {
        self.0.read().is_ok_and(|tracked| tracked.contains(mint))
    }

    pub fn apply(&self, command: WatchlistCommand) -> usize {
        let mut tracked = self.0.write().unwrap_or_else(|err| err.into_inner());
        match command {
            WatchlistCommand::Add(mint) => {
                if tracked.len() < MAX_TRACKED_MINTS {
                    tracked.insert(mint);
                }
            }
            WatchlistCommand::Remove(mint) => {
                tracked.remove(&mint);
            }
            WatchlistCommand::Replace(mints) => {
                *tracked = mints.into_iter().take(MAX_TRACKED_MINTS).collect();
            }
        }
        tracked.len()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum WatchlistCommand {
    Add(Pubkey),
    Remove(Pubkey),
    Replace(HashSet<Pubkey>),
}

pub fn parse_watchlist_command(input: &[u8]) -> Result<WatchlistCommand, String> {
    let input = std::str::from_utf8(input).map_err(|err| format!("command is not UTF-8: {err}"))?;
    let mut fields = input
        .trim()
        .split(|ch: char| ch.is_ascii_whitespace() || ch == ',')
        .filter(|field| !field.is_empty());
    let verb = fields
        .next()
        .ok_or_else(|| "empty watchlist command".to_string())?
        .to_ascii_uppercase();
    let parse_mint =
        |value: &str| Pubkey::from_str(value).map_err(|err| format!("invalid mint {value}: {err}"));
    match verb.as_str() {
        "ADD" => {
            let mint = fields
                .next()
                .ok_or_else(|| "ADD needs one mint".to_string())?;
            if fields.next().is_some() {
                return Err("ADD accepts exactly one mint".to_string());
            }
            Ok(WatchlistCommand::Add(parse_mint(mint)?))
        }
        "REMOVE" => {
            let mint = fields
                .next()
                .ok_or_else(|| "REMOVE needs one mint".to_string())?;
            if fields.next().is_some() {
                return Err("REMOVE accepts exactly one mint".to_string());
            }
            Ok(WatchlistCommand::Remove(parse_mint(mint)?))
        }
        "REPLACE" | "REPLACE_WATCHLIST" => Ok(WatchlistCommand::Replace(
            fields.map(parse_mint).collect::<Result<_, _>>()?,
        )),
        _ => Err(format!("unknown watchlist command {verb}")),
    }
}

#[inline]
pub fn is_pumpfun_buy(program_id: &Pubkey, data: &[u8]) -> bool {
    static PROGRAM_ID: OnceLock<Pubkey> = OnceLock::new();
    program_id
        == PROGRAM_ID.get_or_init(|| Pubkey::from_str(PUMPFUN_PROGRAM).expect("valid Pump program"))
        && data.starts_with(&BUY_DISCRIMINATOR)
}

pub fn detect_tracked_buys(
    slot: u64,
    transaction: &VersionedTransaction,
    watchlist: &Watchlist,
) -> Vec<TrackedBuy> {
    let static_keys = transaction.message.static_account_keys();
    let Some(signature) = transaction.signatures.first().copied() else {
        return Vec::new();
    };
    let recent_blockhash = *transaction.message.recent_blockhash();
    let observed_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos() as u64);
    transaction
        .message
        .instructions()
        .iter()
        .filter_map(|instruction| {
            let program_id = static_keys.get(usize::from(instruction.program_id_index))?;
            if !is_pumpfun_buy(program_id, &instruction.data) || instruction.accounts.len() < 7 {
                return None;
            }
            // Pump's classic top-level `buy` layout is:
            // global, fee recipient, mint, bonding curve, associated bonding curve,
            // associated user, user, ... . Loaded v0 addresses cannot be resolved
            // without an ALT source, so only static-key indices are accepted here.
            let account = |position: usize| {
                instruction
                    .accounts
                    .get(position)
                    .and_then(|index| static_keys.get(usize::from(*index)))
                    .copied()
            };
            let mint = account(2)?;
            if !watchlist.contains(&mint) {
                return None;
            }
            let read_u64 = |offset: usize| {
                instruction
                    .data
                    .get(offset..offset + 8)?
                    .try_into()
                    .ok()
                    .map(u64::from_le_bytes)
            };
            Some(TrackedBuy {
                observed_ns,
                slot,
                signature,
                mint,
                bonding_curve: account(3)?,
                associated_bonding_curve: account(4)?,
                buyer: account(6)?,
                token_amount: read_u64(8)?,
                max_sol_cost: read_u64(16)?,
                recent_blockhash,
                instruction_data: instruction.data.clone(),
            })
        })
        .collect()
}

#[derive(Default)]
pub struct SignatureDedupe {
    slots: BTreeMap<u64, HashSet<Signature>>,
    slot_window: u64,
}

impl SignatureDedupe {
    pub fn new(slot_window: u64) -> Self {
        Self {
            slot_window,
            ..Self::default()
        }
    }

    pub fn insert(&mut self, slot: u64, signature: Signature) -> bool {
        let seen = self.slots.entry(slot).or_default();
        let inserted = if seen.len() < MAX_SIGNATURES_PER_SLOT {
            seen.insert(signature)
        } else {
            false
        };
        let floor = slot.saturating_sub(self.slot_window);
        self.slots.retain(|seen_slot, _| *seen_slot >= floor);
        inserted
    }
}

pub const SLOT_WINDOW: u64 = 8;
pub const MAX_FEC_SETS: usize = 256;
pub const MAX_DATA_SHREDS_PER_SLOT: usize = 4096;

#[derive(Default)]
pub struct BoundedFecState<T> {
    pub slots: BTreeMap<u64, BTreeMap<u32, T>>,
}

impl<T> BoundedFecState<T> {
    pub fn insert(&mut self, slot: u64, fec_set: u32, value: T) {
        self.slots.entry(slot).or_default().insert(fec_set, value);
        let floor = slot.saturating_sub(SLOT_WINDOW);
        self.slots.retain(|state_slot, _| *state_slot >= floor);
        while self.len() > MAX_FEC_SETS {
            let Some((&oldest_slot, &oldest_fec)) = self
                .slots
                .iter()
                .next()
                .and_then(|(slot, sets)| sets.keys().next().map(|fec| (slot, fec)))
            else {
                break;
            };
            let sets = self.slots.get_mut(&oldest_slot).unwrap();
            sets.remove(&oldest_fec);
            if sets.is_empty() {
                self.slots.remove(&oldest_slot);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.slots.values().map(BTreeMap::len).sum()
    }
}

pub type FecShreds = HashMap<solana_ledger::shred::ShredId, solana_ledger::shred::Shred>;

pub fn decode_entries(shreds: impl IntoIterator<Item = solana_ledger::shred::Shred>) -> Vec<Entry> {
    let mut shreds: Vec<_> = shreds.into_iter().filter(|shred| shred.is_data()).collect();
    shreds.sort_unstable_by_key(|shred| shred.index());
    solana_ledger::shred::Shredder::deshred(shreds.iter().map(|shred| shred.payload()))
        .ok()
        .and_then(|payload| bincode::deserialize::<Vec<Entry>>(&payload).ok())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use {
        super::*, solana_hash::Hash, solana_instruction::CompiledInstruction,
        solana_message::Message, solana_transaction::Transaction,
    };

    fn pubkey(byte: u8) -> Pubkey {
        Pubkey::new_from_array([byte; 32])
    }

    fn buy_transaction(mint: Pubkey) -> VersionedTransaction {
        let program = Pubkey::from_str(PUMPFUN_PROGRAM).unwrap();
        let payer = pubkey(1);
        let keys = [pubkey(2), pubkey(3), mint, pubkey(5), pubkey(6), pubkey(7)];
        let mut data = BUY_DISCRIMINATOR.to_vec();
        data.extend_from_slice(&123u64.to_le_bytes());
        data.extend_from_slice(&456u64.to_le_bytes());
        let instruction =
            CompiledInstruction::new_from_raw_parts(7, data, vec![1, 2, 3, 4, 5, 6, 0]);
        let message = Message::new_with_compiled_instructions(
            1,
            0,
            7,
            [payer].into_iter().chain(keys).chain([program]).collect(),
            Hash::new_from_array([9; 32]),
            vec![instruction],
        );
        VersionedTransaction::from(Transaction {
            signatures: vec![Signature::from([8; 64])],
            message,
        })
    }

    #[test]
    fn discriminator_parsing_is_buy_only() {
        let program = Pubkey::from_str(PUMPFUN_PROGRAM).unwrap();
        assert!(is_pumpfun_buy(&program, &BUY_DISCRIMINATOR));
        assert!(!is_pumpfun_buy(&program, &[0; 8]));
        assert!(!is_pumpfun_buy(&pubkey(42), &BUY_DISCRIMINATOR));
    }

    #[test]
    fn resolves_mint_and_buy_fields() {
        let mint = pubkey(4);
        let watchlist = Watchlist::default();
        watchlist.apply(WatchlistCommand::Add(mint));
        let buys = detect_tracked_buys(99, &buy_transaction(mint), &watchlist);
        assert_eq!(buys.len(), 1);
        assert_eq!(buys[0].mint, mint);
        assert_eq!(buys[0].token_amount, 123);
        assert_eq!(buys[0].max_sol_cost, 456);
        assert_eq!(buys[0].signature, Signature::from([8; 64]));
    }

    #[test]
    fn watchlist_add_remove_replace() {
        let watchlist = Watchlist::default();
        let first = pubkey(10);
        let second = pubkey(11);
        watchlist.apply(WatchlistCommand::Add(first));
        assert!(watchlist.contains(&first));
        watchlist.apply(WatchlistCommand::Remove(first));
        assert!(!watchlist.contains(&first));
        watchlist.apply(WatchlistCommand::Replace(HashSet::from([second])));
        assert!(!watchlist.contains(&first));
        assert!(watchlist.contains(&second));
    }

    #[test]
    fn event_encoding_is_stable_and_versioned() {
        let mint = pubkey(4);
        let watchlist = Watchlist::default();
        watchlist.apply(WatchlistCommand::Add(mint));
        let event = detect_tracked_buys(99, &buy_transaction(mint), &watchlist).remove(0);
        let encoded = event.encode().unwrap();
        assert_eq!(&encoded[..4], &EVENT_MAGIC);
        assert_eq!(encoded[4], BUY_TAG);
        assert_eq!(u64::from_le_bytes(encoded[13..21].try_into().unwrap()), 99);
        assert_eq!(&encoded[21..85], &[8; 64]);
        assert_eq!(&encoded[85..117], mint.as_ref());
        assert_eq!(encoded.len(), EVENT_FIXED_LEN + 24);
    }

    #[test]
    fn dedupe_is_bounded_by_slot() {
        let signature = Signature::from([3; 64]);
        let mut dedupe = SignatureDedupe::new(2);
        assert!(dedupe.insert(10, signature));
        assert!(!dedupe.insert(10, signature));
        assert!(dedupe.insert(13, signature));
    }

    #[test]
    fn fec_state_is_bounded() {
        let mut state = BoundedFecState::default();
        for index in 0..(MAX_FEC_SETS + 10) {
            state.insert(100, index as u32, index);
        }
        assert_eq!(state.len(), MAX_FEC_SETS);
        state.insert(100 + SLOT_WINDOW + 1, 0, 0);
        assert_eq!(state.slots.keys().copied().collect::<Vec<_>>(), vec![109]);
    }

    #[test]
    fn parses_control_alias_and_empty_replace() {
        let mint = pubkey(12);
        assert_eq!(
            parse_watchlist_command(format!("ADD {mint}").as_bytes()).unwrap(),
            WatchlistCommand::Add(mint)
        );
        assert_eq!(
            parse_watchlist_command(b"REPLACE_WATCHLIST").unwrap(),
            WatchlistCommand::Replace(HashSet::new())
        );
    }
}
