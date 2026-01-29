use std::collections::HashMap;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use parking_lot::RwLock;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use solana_account::{Account, AccountSharedData};
use solana_pubkey::Pubkey;
use solana_rpc_client::rpc_client::RpcClient;

/// Scenario manages account overrides with automatic persistence.
/// It stores accounts as AccountSharedData internally but serializes as Account.
/// When an RPC client is provided, missing accounts are fetched and persisted.
pub struct Scenario {
    should_persist: AtomicBool,
    pub(crate) allow_uninitialized_accounts: bool,
    dirty: AtomicBool,
    data: Arc<RwLock<HashMap<Pubkey, AccountSharedData>>>,
    path: Option<PathBuf>,
    rpc_client: Option<Arc<RpcClient>>,
}

impl Default for Scenario {
    fn default() -> Self {
        Self {
            should_persist: AtomicBool::new(false),
            allow_uninitialized_accounts: false,
            dirty: AtomicBool::new(false),
            data: Arc::new(RwLock::new(HashMap::new())),
            path: None,
            rpc_client: None,
        }
    }
}

impl Clone for Scenario {
    fn clone(&self) -> Self {
        Self {
            should_persist: AtomicBool::new(self.should_persist.load(Ordering::Relaxed)),
            allow_uninitialized_accounts: self.allow_uninitialized_accounts,
            dirty: AtomicBool::new(self.dirty.load(Ordering::Relaxed)),
            // Deep copy data - each clone gets its own isolated copy
            // This is necessary for parallel execution where workers may write different overrides
            data: Arc::new(RwLock::new(self.data.read().clone())),
            path: self.path.clone(),
            rpc_client: self.rpc_client.clone(),
        }
    }
}

#[serde_as]
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
struct SerializableScenario(
    #[serde_as(as = "HashMap<serde_with::DisplayFromStr, AccountAsJsonAccount>")]
    HashMap<Pubkey, Account>,
);

#[serde_as]
#[derive(Serialize, Deserialize)]
struct JsonAccount {
    #[serde(default)]
    pub lamports: u64,
    #[serde_as(as = "serde_with::hex::Hex")]
    pub data: Vec<u8>,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub owner: Pubkey,
    #[serde(default)]
    pub executable: bool,
    #[serde(default)]
    pub rent_epoch: u64,
}

impl From<JsonAccount> for Account {
    fn from(value: JsonAccount) -> Self {
        Account {
            lamports: value.lamports,
            data: value.data,
            owner: value.owner,
            executable: value.executable,
            rent_epoch: value.rent_epoch,
        }
    }
}

impl From<Account> for JsonAccount {
    fn from(value: Account) -> Self {
        JsonAccount {
            lamports: value.lamports,
            data: value.data,
            owner: value.owner,
            executable: value.executable,
            rent_epoch: value.rent_epoch,
        }
    }
}

serde_with::serde_conv!(
    AccountAsJsonAccount,
    Account,
    |account: &Account| { JsonAccount::from(account.clone()) },
    |account: JsonAccount| -> Result<_, std::convert::Infallible> { Ok(account.into()) }
);

impl Scenario {
    /// Load a scenario from a file, or create an empty one if the file doesn't exist.
    pub fn from_file(path: PathBuf, allow_uninitialized_accounts: bool) -> Self {
        let data = if path.exists() {
            let serializable: SerializableScenario = read_json_gz(&path);
            serializable
                .0
                .into_iter()
                .map(|(pubkey, account)| (pubkey, account.into()))
                .collect()
        } else {
            HashMap::new()
        };

        Scenario {
            should_persist: AtomicBool::new(true),
            allow_uninitialized_accounts,
            dirty: AtomicBool::new(false),
            data: Arc::new(RwLock::new(data)),
            path: Some(path),
            rpc_client: None,
        }
    }

    /// Load a scenario with RPC fallback enabled.
    pub fn from_file_with_rpc(
        path: PathBuf,
        rpc_url: String,
        allow_uninitialized_accounts: bool,
    ) -> Self {
        let mut scenario = Self::from_file(path, allow_uninitialized_accounts);
        scenario.rpc_client = Some(Arc::new(RpcClient::new(rpc_url)));
        scenario
    }

    pub fn rpc_only(rpc_url: String, allow_uninitialized_accounts: bool) -> Self {
        Scenario {
            should_persist: AtomicBool::new(false),
            allow_uninitialized_accounts,
            dirty: AtomicBool::new(false),
            data: Arc::new(RwLock::new(HashMap::new())),
            path: None,
            rpc_client: Some(Arc::new(RpcClient::new(rpc_url))),
        }
    }

    /// Fetch an account from RPC and store it in the scenario.
    /// Panics if RPC is not configured or if the RPC request fails.
    pub fn must_fetch_from_rpc(&self, pubkey: &Pubkey) -> AccountSharedData {
        self.try_fetch_from_rpc(pubkey).unwrap()
    }

    pub fn try_fetch_from_rpc(&self, pubkey: &Pubkey) -> Option<AccountSharedData> {
        log::debug!("Attempting to fetch account: {pubkey}");
        let rpc_client = self.rpc_client.as_ref().expect(
            format!("Account {pubkey} not found in scenario or accounts. RPC URL must be configured to fetch \
             missing accounts.").as_str(),
        );

        match rpc_client.get_account(pubkey) {
            Ok(account) => {
                let account_shared: AccountSharedData = account.into();
                self.dirty.store(true, Ordering::Relaxed);
                self.data.write().insert(*pubkey, account_shared.clone());
                Some(account_shared)
            }
            // For AccountNotFound, return None if uninitialized accounts are allowed
            Err(err)
                if err.to_string().contains("AccountNotFound")
                    && self.allow_uninitialized_accounts =>
            {
                log::debug!(
                    "Account not found on RPC: {pubkey}. Returning default uninitialized account."
                );
                Some(AccountSharedData::default())
            }
            Err(_) => None,
        }
    }

    pub fn get(&self, pubkey: &Pubkey) -> Option<AccountSharedData> {
        self.data.read().get(pubkey).cloned()
    }

    pub fn insert(&mut self, pubkey: Pubkey, account: AccountSharedData) {
        self.dirty.store(true, Ordering::Relaxed);
        self.data.write().insert(pubkey, account);
    }

    pub fn rpc_enabled(&self) -> bool {
        self.rpc_client.is_some()
    }
}

impl Drop for Scenario {
    fn drop(&mut self) {
        if self.dirty.load(Ordering::Relaxed) && self.should_persist.load(Ordering::Relaxed) {
            if let Some(path) = &self.path {
                // Convert AccountSharedData back to Account for serialization
                let accounts: HashMap<Pubkey, Account> = self
                    .data
                    .read()
                    .iter()
                    .map(|(pubkey, account_shared)| (*pubkey, account_shared.clone().into()))
                    .collect();

                let serializable = SerializableScenario(accounts);

                // Ensure the parent directory exists
                if let Some(parent) = path.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }

                try_write_json_gz(path, &serializable);
            }
        }
    }
}

pub fn try_write_json_gz<T>(path: &Path, data: &T)
where
    T: Serialize,
{
    let file = match std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(path)
    {
        Ok(file) => file,
        Err(err) => {
            eprintln!("Failed to write to file; path={path:?}; err={err}");
            return;
        }
    };
    let compression = GzEncoder::new(file, flate2::Compression::best());

    match serde_json::to_writer(compression, &data) {
        Ok(serialized) => serialized,
        Err(err) => {
            eprintln!("Failed to serialize data; path={path:?}; err={err}");
        }
    }
}

pub fn read_json_gz<T>(path: &Path) -> T
where
    T: DeserializeOwned,
{
    let compressed = open_read(path);
    let bytes = BufReader::new(GzDecoder::new(compressed));

    serde_json::from_reader(bytes).unwrap()
}

fn open_read(path: &Path) -> std::fs::File {
    std::fs::OpenOptions::new()
        .read(true)
        .open(path)
        .unwrap_or_else(|err| panic!("Failed to open file; path={path:?}; err={err}"))
}
