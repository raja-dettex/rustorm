use std::process::exit;

use crate::{database::DatabaseClient, sql::Statement};

pub struct Transaction { 
    isolation_level: IsolationLevel,
    client : DatabaseClient,
}

pub enum IsolationLevel { 
    ReadCommited
}
#[derive(Debug)]
pub enum TransactionErr {
    Begin(ErrBegin),
    RollBack(ErrRollback),
    Commit(ErrCommit)
}
#[derive(Debug)]
pub struct ErrBegin { 
    msg: String
}
#[derive(Debug)]
pub struct ErrRollback {
    msg: String
}
#[derive(Debug)]
pub struct ErrCommit { 
    msg: String
}

impl Transaction { 
    pub async fn begin(isolation_level: Option<IsolationLevel>, client: DatabaseClient) -> Result<Self, TransactionErr> { 
        let isolation = match isolation_level {
            Some(isolation) => isolation,
            None => IsolationLevel::ReadCommited
        };
        print!("begin");
        if let Err(err) = client.execute(Statement::new("BEGIN;".to_string(), None)).await {
            return Err(TransactionErr::Begin(ErrBegin{ msg: err.to_string()}));
        }
        println!("set transaction");
        if let Err(err) = match isolation { 
            IsolationLevel::ReadCommited => client.execute(Statement::new(format!("set transaction isolation level read committed"), None)).await
        } {
            return Err(TransactionErr::Begin(ErrBegin{msg: err.to_string()}));
        }
        println!("done");
        Ok(Self { isolation_level: isolation , client})
    }

    pub async fn execute(&mut self , statement: Statement) -> Result<(), TransactionErr>{ 
        println!("executing");
        if let Err(err) = self.client.execute(statement).await { 
            self.rollback().await;
            return Err(TransactionErr::Begin(ErrBegin{msg: err.to_string()}));
        }
        println!("execution done");
        Ok(())
    }

    pub async fn commit(&mut self) -> Result<(), TransactionErr>{
        println!("commit"); 
        if let Err(err) = self.client.execute(Statement::new("commit;".to_string(), None)).await {
            println!("error commit : {}", err.to_string());
            self.rollback().await;
            return Err(TransactionErr::Commit(ErrCommit{msg: err.to_string()}));
        }
        println!("commit done");
        Ok(())
    }

    pub async fn rollback(&mut self) -> Result<(), TransactionErr>{ 
        if let Err(err) = self.client.execute(Statement::new("rollback;".to_string(), None)).await {
            return Err(TransactionErr::RollBack(ErrRollback { msg: err.to_string() }))
        }
        Ok(())
    }

    
}


#[tokio::test]
pub async fn test_transaction() {
    use crate::sql::Parameter;
    use crate::pool::ConnectionPool;
    let params1 = vec![
        Parameter::Int(1),
        Parameter::StringValue("raja".to_string())
    ];
    let params2 = vec![
        Parameter::Int(2),
        Parameter::StringValue("devi".to_string())
    ];
    let st1 = Statement::new("insert into users values(?,?)".to_string(), Some(params1));
    let st2 = Statement::new("insert into users values(?,?)".to_string(), Some(params2));

    let pool = ConnectionPool::new("postgresql://postgres:raja@localhost:5432/demo-db", 4).await.unwrap();
    let db_client = DatabaseClient::new(pool);
    match Transaction::begin(None, db_client).await {
        Ok(mut transaction) => {
            if let Err(err) = transaction.execute(st1).await { 
                println!("execution error: {err:?}");
                exit(0);
            }  
            if let Err(err) = transaction.execute(st2).await { 
                println!("execution error: {err:?}");
                exit(0);
            }
            if let Err(err) = transaction.commit().await { 
                println!("execution error: {err:?}");
                exit(0);
            }
        },    
        Err(begin_err) => {
            println!("begin error : {begin_err:?}");
            exit(0);
        }
    } 
    
}