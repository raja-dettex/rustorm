use crate::error::InvalidSql;
pub struct Row {
    pub columns : Vec<Column>
}


impl IntoIterator for Row {
    type Item = ColumnValue;

    type IntoIter = RowIterator;

    fn into_iter(self) -> Self::IntoIter {
        let values: Vec<Option<ColumnValue>>= self.columns.iter().map(|column| column.clone().value).collect();
        RowIterator::new(values)
    }
}

pub struct RowIterator {
    values: Vec<Option<ColumnValue>>,
    index: usize
}

impl FromIterator<ColumnValue> for RowIterator {
    fn from_iter<T: IntoIterator<Item = ColumnValue>>(iter: T) -> Self {
        let values = iter.into_iter().map(Some).collect();
        RowIterator{values, index: 0}
    }
}

impl RowIterator {
    pub fn new(values: Vec<Option<ColumnValue>>) -> Self {
        Self {values, index: 0}
    }
}

impl Iterator for RowIterator {
    type Item = ColumnValue;

    fn next(&mut self) -> Option<Self::Item> {
        while self.index < self.values.len() {
            if let Some(Some(value)) = self.values.get(self.index) {
                self.index += 1;
                return Some(value.clone());
            }
            return None;
        }
        None
     }
}


#[derive(Debug, Clone)]
pub struct Column { 
    pub value: Option<ColumnValue>,
    pub meta: Option<ColumnMeta>
}


#[derive(Debug,Clone)]
pub struct ColumnMeta {
    pub name: String
}

#[derive(Debug,Clone)]
pub enum ColumnValue { 
    String(String),
    Int(i32),
    Blob(Vec<u8>)
}

pub struct Statement {
    statement: String,
    params: Option<Vec<Parameter>>
}

pub enum Parameter { 
    Column(String),
    StringValue(String),
    Int(i32),
    Blob(Vec<u8>) 
}


impl Clone for Parameter {
    fn clone(&self) -> Self {
        match self {
            Self::Column(arg0) => Self::Column(arg0.clone()),
            Self::StringValue(arg0) => Self::StringValue(arg0.clone()),
            Self::Int(arg0) => Self::Int(arg0.clone()),
            Self::Blob(arg0) => Self::Blob(arg0.clone()),
        }
    }
}


impl Statement { 
    pub fn new(statement: String, params: Option<Vec<Parameter>>) -> Self {
        Self{statement, params}
    }
    pub fn tosql(&self) -> Result<String, InvalidSql> { 
        let count = self.statement.chars().filter(|&c| c == '?').count();
        if self.params.is_none() && count == 0 {
            return Ok(self.statement.to_string());
        }
        else if self.params.is_none() && count != 0 {
            return Err(InvalidSql{msg: "invalid sql with extra placeholder ?".to_string()});
        } 
        else if !self.params.is_none() && count == 0 {
            return Err(InvalidSql{msg: "invalid sql with extra placeholder parameters".to_string()});
        } 
        let mut result = String::new();
        let mut idx = 0;
        for c in self.statement.chars() {
            if c == '?' {
                if let Some(param) = self.params.clone().unwrap().get(idx) {
                    match param {
                        Parameter::Column(str) => result.push_str(&format!("{}", str)),
                        Parameter::StringValue(str) => result.push_str(&format!("'{}'", str)),
                        Parameter::Int(int) => result.push_str(&format!("{}", int)),
                        Parameter::Blob(items) => todo!(),
                    }
                    ; // Properly wrap values in quotes
                    idx += 1;
                }
            } else {
                result.push(c);
            }
        }
        Ok(result)
    }
}
