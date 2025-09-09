use serde_redis::{Array, Value};

/// Represents every command in the transaction.
#[derive(Debug)]
pub(crate) struct TransactionEvent {
    /// Command name.
    pub(crate) cmd: String,

    /// Command args.
    pub(crate) args: Array,
}

impl TransactionEvent {
    pub(crate) fn new(cmd: impl Into<String>, args: Array) -> Self {
        Self {
            cmd: cmd.into(),
            args,
        }
    }
}

/// Transaction is state machine represents all state can
/// stay in.
#[derive(Debug)]
pub(crate) enum Transaction {
    /// Outside of any transaction.
    None,

    /// Inside a transaction process, now it's recording
    /// all incoming `TransactionEvent`s and waiting for
    /// submit, which usually an `EXEC` command.
    Pending(Vec<TransactionEvent>),

    /// Excuting commands. This state only occurs when submitting a transaction.
    ///
    /// Stores all command execution result when submitting transaction.
    /// Because the we have to return all those command results in an
    /// array, this is a hijack layer to change the behavior when
    /// outside transaction to inside transaction.
    ///
    /// That is, sending command results back to client outside transaction
    /// like what we did in early stages, this buffer is never used and the
    /// result is sent back directly. But if in a transaction, results are
    /// temporarily stored in the buffer, then waiting for the running `EXEC`
    /// command to take them out and send back in a response.
    ///
    /// So this buffer only used when submitting transaction.
    Executing(Vec<Value>),
}

impl Transaction {
    pub(crate) fn new() -> Self {
        Self::None
    }

    pub fn is_pending(&self) -> bool {
        match self {
            Transaction::None | Transaction::Executing(..) => false,
            Transaction::Pending(..) => true,
        }
    }

    pub fn is_executing(&self) -> bool {
        match self {
            Transaction::None | Transaction::Pending(..) => false,
            Transaction::Executing(..) => true,
        }
    }

    pub fn start(&mut self) {
        match self {
            Transaction::None => *self = Transaction::Pending(vec![]),
            _ => unreachable!("only start a transaction when it's inactive"),
        }
    }

    pub fn commit(&mut self) -> Vec<TransactionEvent> {
        match self {
            Transaction::Pending(cmdlines) => {
                let events = std::mem::replace(cmdlines, vec![]);
                *self = Transaction::Executing(vec![]);
                events
            }
            _ => unreachable!("only submit a transaction when it's pending"),
        }
    }

    pub fn finish(&mut self) -> Vec<Value> {
        match self {
            Transaction::Executing(result) => {
                let result = std::mem::replace(result, vec![]);
                *self = Transaction::None;
                result
            }
            _ => unreachable!("only retrieve the results when executing"),
        }
    }

    pub fn record_result(&mut self, value: Value) {
        match self {
            Transaction::Executing(buf) => buf.push(value),
            Transaction::None | Transaction::Pending(..) => {
                unreachable!("only record result when executing")
            }
        }
    }

    pub fn abort(&mut self) {
        *self = Transaction::None
    }
}
