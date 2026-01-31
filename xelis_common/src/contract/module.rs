use std::sync::Arc;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use xelis_vm::{NumberType, TypePacked};
use crate::serializer::*;
use super::ContractVersion;

pub use xelis_vm::Module;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ContractModule {
    pub version: ContractVersion,
    // keep it behind Arc to reduce cloning overhead
    pub module: Arc<Module>,
}

impl Serializer for ContractModule {
    fn write(&self, writer: &mut Writer) {
        self.version.write(writer);

        writer.context_mut().store(self.version);
        self.module.write(writer);
    }

    fn read(reader: &mut Reader) -> Result<Self, ReaderError> {
        let version = ContractVersion::read(reader)?;

        // Store the version in the context for later use
        reader.context_mut().store(version);

        let module = Module::read(reader)?;

        Ok(Self {
            version,
            module: Arc::new(module),
        })
    }

    fn size(&self) -> usize {
        self.version.size() + self.module.size()
    }
}

impl Serializer for TypePacked {
    fn write(&self, writer: &mut Writer) {
        match self {
            TypePacked::Number(NumberType::U8) => writer.write_u8(0),
            TypePacked::Number(NumberType::U16) => writer.write_u8(1),
            TypePacked::Number(NumberType::U32) => writer.write_u8(2),
            TypePacked::Number(NumberType::U64) => writer.write_u8(3),
            TypePacked::Number(NumberType::U128) => writer.write_u8(4),
            TypePacked::Number(NumberType::U256) => writer.write_u8(5),
            TypePacked::Bool => writer.write_u8(6),
            TypePacked::Bytes => writer.write_u8(7),
            TypePacked::String => writer.write_u8(8),
            TypePacked::Opaque(id) => {
                writer.write_u8(9);
                writer.write_u16(*id);
            },
            TypePacked::Range(inner) => match **inner {
                NumberType::U8 => writer.write_u8(10),
                NumberType::U16 => writer.write_u8(11),
                NumberType::U32 => writer.write_u8(12),
                NumberType::U64 => writer.write_u8(13),
                NumberType::U128 => writer.write_u8(14),
                NumberType::U256 => writer.write_u8(15),
            },
            TypePacked::Array(inner) => {
                writer.write_u8(16);
                inner.write(writer);
            },
            TypePacked::Tuples(fields) => {
                writer.write_u8(17);
                writer.write_u8(fields.len() as u8);
                for field in fields {
                    field.write(writer);
                }
            },
            TypePacked::Map(key, value) => {
                writer.write_u8(18);
                key.write(writer);
                value.write(writer);
            },
            TypePacked::Optional(inner) => {
                writer.write_u8(19);
                inner.write(writer);
            },
            TypePacked::Any => writer.write_u8(20),
            TypePacked::OneOf(variants) => {
                writer.write_u8(21);
                writer.write_u8(variants.len() as u8);
                for variant in variants {
                    writer.write_u8(variant.len() as u8);
                    for v in variant {
                        v.write(writer);
                    }
                }
            }
        }
    }

    fn read(reader: &mut Reader) -> Result<Self, ReaderError> {
        enum WorkItem {
            ReadType,
            BuildArray,
            BuildTuples { remaining: usize, fields: Vec<TypePacked> },
            BuildMapKey,
            BuildMapValue { key: TypePacked },
            BuildOptional,
            BuildOneOf { 
                remaining_variants: usize, 
                variants: Vec<Vec<TypePacked>>,
                current_variant_remaining: Option<usize>,
                current_variant: Vec<TypePacked>
            },
        }

        let mut stack = vec![WorkItem::ReadType];
        let mut tmp = Vec::new();

        while let Some(work) = stack.pop() {
            match work {
                WorkItem::ReadType => {
                    let tag = reader.read_u8()?;
                    match tag {
                        0 => tmp.push(TypePacked::Number(NumberType::U8)),
                        1 => tmp.push(TypePacked::Number(NumberType::U16)),
                        2 => tmp.push(TypePacked::Number(NumberType::U32)),
                        3 => tmp.push(TypePacked::Number(NumberType::U64)),
                        4 => tmp.push(TypePacked::Number(NumberType::U128)),
                        5 => tmp.push(TypePacked::Number(NumberType::U256)),
                        6 => tmp.push(TypePacked::Bool),
                        7 => tmp.push(TypePacked::Bytes),
                        8 => tmp.push(TypePacked::String),
                        9 => {
                            let id = reader.read_u16()?;
                            tmp.push(TypePacked::Opaque(id));
                        },
                        10 => tmp.push(TypePacked::Range(Box::new(NumberType::U8))),
                        11 => tmp.push(TypePacked::Range(Box::new(NumberType::U16))),
                        12 => tmp.push(TypePacked::Range(Box::new(NumberType::U32))),
                        13 => tmp.push(TypePacked::Range(Box::new(NumberType::U64))),
                        14 => tmp.push(TypePacked::Range(Box::new(NumberType::U128))),
                        15 => tmp.push(TypePacked::Range(Box::new(NumberType::U256))),
                        16 => {
                            stack.push(WorkItem::BuildArray);
                            stack.push(WorkItem::ReadType);
                        },
                        17 => {
                            let len = reader.read_u8()? as usize;
                            if len == 0 {
                                tmp.push(TypePacked::Tuples(Vec::new()));
                            } else {
                                stack.push(WorkItem::BuildTuples { remaining: len, fields: Vec::with_capacity(len) });
                                stack.push(WorkItem::ReadType);
                            }
                        },
                        18 => {
                            stack.push(WorkItem::BuildMapKey);
                            stack.push(WorkItem::ReadType);
                        },
                        19 => {
                            stack.push(WorkItem::BuildOptional);
                            stack.push(WorkItem::ReadType);
                        },
                        20 => tmp.push(TypePacked::Any),
                        21 => {
                            let len = reader.read_u8()? as usize;
                            if len == 0 {
                                tmp.push(TypePacked::OneOf(Vec::new()));
                            } else {
                                let first_variant_len = reader.read_u8()? as usize;
                                if first_variant_len == 0 {
                                    stack.push(WorkItem::BuildOneOf {
                                        remaining_variants: len - 1,
                                        variants: Vec::with_capacity(len),
                                        current_variant_remaining: None,
                                        current_variant: Vec::new()
                                    });
                                } else {
                                    stack.push(WorkItem::BuildOneOf {
                                        remaining_variants: len - 1,
                                        variants: Vec::with_capacity(len),
                                        current_variant_remaining: Some(first_variant_len),
                                        current_variant: Vec::with_capacity(first_variant_len)
                                    });
                                    stack.push(WorkItem::ReadType);
                                }
                            }
                        },
                        _ => return Err(ReaderError::InvalidValue),
                    }
                },
                WorkItem::BuildArray => {
                    let inner = tmp.pop().ok_or(ReaderError::InvalidValue)?;
                    tmp.push(TypePacked::Array(Box::new(inner)));
                },
                WorkItem::BuildTuples { remaining, mut fields } => {
                    let field = tmp.pop().ok_or(ReaderError::InvalidValue)?;
                    fields.push(field);
                    
                    if remaining > 1 {
                        stack.push(WorkItem::BuildTuples { remaining: remaining - 1, fields });
                        stack.push(WorkItem::ReadType);
                    } else {
                        tmp.push(TypePacked::Tuples(fields));
                    }
                },
                WorkItem::BuildMapKey => {
                    let key = tmp.pop().ok_or(ReaderError::InvalidValue)?;
                    stack.push(WorkItem::BuildMapValue { key });
                    stack.push(WorkItem::ReadType);
                },
                WorkItem::BuildMapValue { key } => {
                    let value = tmp.pop().ok_or(ReaderError::InvalidValue)?;
                    tmp.push(TypePacked::Map(Box::new(key), Box::new(value)));
                },
                WorkItem::BuildOptional => {
                    let inner = tmp.pop().ok_or(ReaderError::InvalidValue)?;
                    tmp.push(TypePacked::Optional(Box::new(inner)));
                },
                WorkItem::BuildOneOf { remaining_variants, mut variants, current_variant_remaining, mut current_variant } => {
                    if let Some(remaining) = current_variant_remaining {
                        let field = tmp.pop().ok_or(ReaderError::InvalidValue)?;
                        current_variant.push(field);
                        
                        if remaining > 1 {
                            stack.push(WorkItem::BuildOneOf {
                                remaining_variants,
                                variants,
                                current_variant_remaining: Some(remaining - 1),
                                current_variant
                            });
                            stack.push(WorkItem::ReadType);
                        } else {
                            variants.push(current_variant);
                            
                            if remaining_variants > 0 {
                                let next_variant_len = reader.read_u8()? as usize;
                                if next_variant_len == 0 {
                                    stack.push(WorkItem::BuildOneOf {
                                        remaining_variants: remaining_variants - 1,
                                        variants,
                                        current_variant_remaining: None,
                                        current_variant: Vec::new()
                                    });
                                } else {
                                    stack.push(WorkItem::BuildOneOf {
                                        remaining_variants: remaining_variants - 1,
                                        variants,
                                        current_variant_remaining: Some(next_variant_len),
                                        current_variant: Vec::with_capacity(next_variant_len)
                                    });
                                    stack.push(WorkItem::ReadType);
                                }
                            } else {
                                tmp.push(TypePacked::OneOf(variants));
                            }
                        }
                    } else {
                        variants.push(current_variant);
                        
                        if remaining_variants > 0 {
                            let next_variant_len = reader.read_u8()? as usize;
                            if next_variant_len == 0 {
                                stack.push(WorkItem::BuildOneOf {
                                    remaining_variants: remaining_variants - 1,
                                    variants,
                                    current_variant_remaining: None,
                                    current_variant: Vec::new()
                                });
                            } else {
                                stack.push(WorkItem::BuildOneOf {
                                    remaining_variants: remaining_variants - 1,
                                    variants,
                                    current_variant_remaining: Some(next_variant_len),
                                    current_variant: Vec::with_capacity(next_variant_len)
                                });
                                stack.push(WorkItem::ReadType);
                            }
                        } else {
                            tmp.push(TypePacked::OneOf(variants));
                        }
                    }
                },
            }
        }

        tmp.pop().ok_or(ReaderError::InvalidValue)
    }

    fn size(&self) -> usize {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_complex_type_packed_serialization() {
        let original = TypePacked::OneOf(vec![
            vec![
                TypePacked::Number(NumberType::U8),
                TypePacked::String,
            ],
            vec![
                TypePacked::Array(Box::new(TypePacked::Bool)),
            ],
            vec![],
        ]);

        let bytes = original.to_bytes();

        let mut reader = Reader::new(&bytes);
        let deserialized = TypePacked::read(&mut reader).expect("Deserialization failed");

        assert_eq!(original, deserialized);
    }
}