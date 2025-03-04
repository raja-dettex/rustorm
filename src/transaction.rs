

use crate::client::ClientWrapper;
use crate::sql::Statement;

pub struct Transaction {
    isolation_level: IsolationLevel,
    pub client: ClientWrapper,
    statements: Vec<Statement>,
}

pub enum IsolationLevel {
    ReadCommited,
}
#[derive(Debug)]
pub enum TransactionErr {
    Begin(ErrBegin),
    RollBack(ErrRollback),
    Commit(ErrCommit),
    Execute(ErrExecute),
}

#[derive(Debug)]
pub struct ErrExecute {
    msg: String,
}

impl ToString for TransactionErr {
    fn to_string(&self) -> String {
        match self {
            Self::Begin(err) => err.msg.to_string(),
            Self::RollBack(err) => err.msg.to_string(),
            Self::Commit(err) => err.msg.to_string(),
            Self::Execute(err) => err.msg.to_string(),
        }
    }
}
#[derive(Debug)]
pub struct ErrBegin {
    msg: String,
}
#[derive(Debug)]
pub struct ErrRollback {
    msg: String,
}
#[derive(Debug)]
pub struct ErrCommit {
    msg: String,
}

impl Transaction {
    pub async fn begin(
        isolation_level: Option<IsolationLevel>,
        client: ClientWrapper,
    ) -> Result<Self, TransactionErr> {
        let isolation = match isolation_level {
            Some(isolation) => isolation,
            None => IsolationLevel::ReadCommited,
        };

        if let Err(err) = client
            .execute(Statement::new("BEGIN;".to_string(), None))
            .await
        {
            return Err(TransactionErr::Begin(ErrBegin {
                msg: err.to_string(),
            }));
        }

        if let Err(err) = match isolation {
            IsolationLevel::ReadCommited => {
                client
                    .execute(Statement::new(
                        format!("set transaction isolation level read committed"),
                        None,
                    ))
                    .await
            }
        } {
            return Err(TransactionErr::Begin(ErrBegin {
                msg: err.to_string(),
            }));
        }
        Ok(Self {
            isolation_level: isolation,
            client,
            statements: Vec::new(),
        })
    }

    pub fn execute(&mut self, statement: Statement) {
        self.statements.push(statement);
    }

    pub async fn commit(&mut self) -> Result<(), TransactionErr> {
        for statement in self.statements.clone() {
            match self.client.execute(statement).await {
                Ok(_) => continue,
                Err(err) => {
                    return Err(TransactionErr::Execute(ErrExecute {
                        msg: err.to_string(),
                    }))
                }
            }
        }
        if let Err(err) = self
            .client
            .execute(Statement::new("commit;".to_string(), None))
            .await
        {
            return Err(TransactionErr::Commit(ErrCommit {
                msg: err.to_string(),
            }));
        }
        Ok(())
    }

    pub async fn rollback(&mut self) -> Result<(), TransactionErr> {
        if let Err(err) = self
            .client
            .execute(Statement::new("rollback;".to_string(), None))
            .await
        {
            return Err(TransactionErr::RollBack(ErrRollback {
                msg: err.to_string(),
            }));
        }
        Ok(())
    }
}
