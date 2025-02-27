
#[derive(Debug)]
pub struct InvalidSql { 
    pub msg: String
}

#[derive(Debug)]
pub enum DatabaseError { 
    InvalidSql(InvalidSql),
    ExecutionError(ExecutionError)
}

#[derive(Debug)]
pub struct ExecutionError { 
    pub msg: String
}

impl ToString for DatabaseError {
    fn to_string(&self) -> String {
        match self { 
            Self::InvalidSql(invalid_sql) => invalid_sql.msg.clone(),
            Self::ExecutionError(execution_err) => execution_err.msg.clone()
        }
    }
}