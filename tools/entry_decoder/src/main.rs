use std::io::{self, Read};

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use serde::Serialize;
use solana_entry::entry::Entry;
use solana_sdk::message::VersionedMessage;
use solana_sdk::transaction::VersionedTransaction;

#[derive(Serialize)]
struct DecodedInstruction {
    program_id: String,
    data_hex: String,
    accounts: Vec<String>,
}

#[derive(Serialize)]
struct DecodedTx {
    signature: Option<String>,
    instructions: Vec<DecodedInstruction>,
}

fn resolve_account_key_v0(
    message: &solana_sdk::message::v0::Message,
    account_index: usize,
) -> String {
    let static_len = message.account_keys.len();
    if account_index < static_len {
        return message.account_keys[account_index].to_string();
    }

    let mut offset = account_index - static_len;
    for lookup in &message.address_table_lookups {
        let table = lookup.account_key.to_string();
        if offset < lookup.writable_indexes.len() {
            let idx = lookup.writable_indexes[offset] as usize;
            return format!("lookup:{table}:w:{idx}");
        }
        offset -= lookup.writable_indexes.len();

        if offset < lookup.readonly_indexes.len() {
            let idx = lookup.readonly_indexes[offset] as usize;
            return format!("lookup:{table}:r:{idx}");
        }
        offset -= lookup.readonly_indexes.len();
    }

    format!("lookup:unknown:{account_index}")
}

fn decode_tx(tx: &VersionedTransaction) -> DecodedTx {
    let signature = tx.signatures.get(0).map(|sig| sig.to_string());
    let mut instructions: Vec<DecodedInstruction> = Vec::new();

    match &tx.message {
        VersionedMessage::Legacy(message) => {
            for ix in &message.instructions {
                let idx = ix.program_id_index as usize;
                if let Some(program_id) = message.account_keys.get(idx) {
                    let accounts = ix
                        .accounts
                        .iter()
                        .map(|acct_idx| {
                            let acct_idx = *acct_idx as usize;
                            if let Some(key) = message.account_keys.get(acct_idx) {
                                key.to_string()
                            } else {
                                format!("lookup:{acct_idx}")
                            }
                        })
                        .collect();
                    instructions.push(DecodedInstruction {
                        program_id: program_id.to_string(),
                        data_hex: hex::encode(&ix.data),
                        accounts,
                    });
                }
            }
        }
        VersionedMessage::V0(message) => {
            for ix in &message.instructions {
                let idx = ix.program_id_index as usize;
                if let Some(program_id) = message.account_keys.get(idx) {
                    let accounts = ix
                        .accounts
                        .iter()
                        .map(|acct_idx| {
                            let acct_idx = *acct_idx as usize;
                            resolve_account_key_v0(message, acct_idx)
                        })
                        .collect();
                    instructions.push(DecodedInstruction {
                        program_id: program_id.to_string(),
                        data_hex: hex::encode(&ix.data),
                        accounts,
                    });
                }
            }
        }
    }

    DecodedTx {
        signature,
        instructions,
    }
}

fn main() {
    let mut input = String::new();
    if io::stdin().read_to_string(&mut input).is_err() {
        eprintln!("failed to read stdin");
        std::process::exit(1);
    }

    let trimmed = input.trim();
    if trimmed.is_empty() {
        eprintln!("empty input");
        std::process::exit(1);
    }

    let bytes = match B64.decode(trimmed.as_bytes()) {
        Ok(b) => b,
        Err(err) => {
            eprintln!("base64 decode error: {err}");
            std::process::exit(1);
        }
    };

    let entries: Vec<Entry> = match bincode::deserialize(&bytes) {
        Ok(v) => v,
        Err(err) => {
            eprintln!("bincode decode error: {err}");
            std::process::exit(1);
        }
    };

    let mut decoded: Vec<DecodedTx> = Vec::new();
    for entry in entries {
        for tx in entry.transactions {
            decoded.push(decode_tx(&tx));
        }
    }

    match serde_json::to_string(&decoded) {
        Ok(json) => {
            print!("{json}");
        }
        Err(err) => {
            eprintln!("json encode error: {err}");
            std::process::exit(1);
        }
    }
}
