
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Type {
    Integer,
    Varchar,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub column_type: Type,
    pub length: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Integer(i32),
    Varchar(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tuple {
    pub values: Vec<Value>,
}

impl Tuple {
    pub fn serialize(&self, schema: &Schema) -> Vec<u8> {
        let mut bytes = Vec::new();
        for (i, value) in self.values.iter().enumerate() {
            let col_type = &schema.columns[i].column_type;
            match (value, col_type) {
                (Value::Integer(val), Type::Integer) => {
                    bytes.extend_from_slice(&val.to_ne_bytes());
                }
                (Value::Varchar(val), Type::Varchar) => {
                    let len = val.len() as u32;
                    bytes.extend_from_slice(&len.to_ne_bytes());
                    bytes.extend_from_slice(val.as_bytes());
                }
                _ => panic!("Type mismatch during serialization"),
            }
        }
        bytes
    }

    pub fn deserialize(bytes: &[u8], schema: &Schema) -> Self {
        let mut values = Vec::new();
        let mut offset = 0;
        for col in &schema.columns {
            match col.column_type {
                Type::Integer => {
                    let val = i32::from_ne_bytes(bytes[offset..offset + 4].try_into().unwrap());
                    values.push(Value::Integer(val));
                    offset += 4;
                }
                Type::Varchar => {
                    let len = u32::from_ne_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;
                    let val = String::from_utf8(bytes[offset..offset + len].to_vec()).unwrap();
                    values.push(Value::Varchar(val));
                    offset += len;
                }
            }
        }
        Tuple { values }
    }
}
