
#[derive(Debug)]
pub struct InvalidSql { 
    pub msg: String
}

#[derive(Debug)]
pub enum DatabaseError { 
    InvalidSql(InvalidSql),
    ExecutionError(ExecutionError),
    ClientNotFound(ClientNotFound)
}

#[derive(Debug, Clone)]
pub struct ClientNotFound { 
   pub msg: String
}
#[derive(Debug)]
pub struct ExecutionError { 
    pub msg: String
}

impl ToString for DatabaseError {
    fn to_string(&self) -> String {
        match self { 
            Self::InvalidSql(invalid_sql) => invalid_sql.msg.clone(),
            Self::ExecutionError(execution_err) => execution_err.msg.clone(),
            Self::ClientNotFound(client_not_found_err) => client_not_found_err.msg.clone()
        }
    }
}