use crate::value::{Ty, Val};
use crate::workload::Field;
use arrow_array::builder::{
    Float32Builder, Float64Builder, Int64Builder, ListBuilder, StringBuilder, UInt8Builder,
    UInt32Builder,
};
use arrow_array::{Array, ArrayRef, RecordBatch};
use std::sync::Arc;

pub enum Col {
    I64(Int64Builder),
    U32(UInt32Builder),
    U8(UInt8Builder),
    F32(Float32Builder),
    F64(Float64Builder),
    Str(StringBuilder),
    List(ListBuilder<Float64Builder>),
}

impl Col {
    pub fn new(ty: Ty, cap: usize) -> Col {
        match ty {
            Ty::I64 => Col::I64(Int64Builder::with_capacity(cap)),
            Ty::U32 => Col::U32(UInt32Builder::with_capacity(cap)),
            Ty::U8 => Col::U8(UInt8Builder::with_capacity(cap)),
            Ty::F32 => Col::F32(Float32Builder::with_capacity(cap)),
            Ty::F64 => Col::F64(Float64Builder::with_capacity(cap)),
            Ty::Str => Col::Str(StringBuilder::with_capacity(cap, cap * 16)),
            Ty::ListF64 => Col::List(ListBuilder::with_capacity(
                Float64Builder::with_capacity(cap * 7),
                cap,
            )),
        }
    }

    #[inline]
    pub fn push<V: Val>(&mut self, v: Option<&V>) {
        match self {
            Col::I64(b) => b.append_option(v.and_then(|v| v.as_i64())),
            Col::U32(b) => b.append_option(v.and_then(|v| v.as_i64()).map(|x| x as u32)),
            Col::U8(b) => b.append_option(v.and_then(|v| v.as_i64()).map(|x| x as u8)),
            Col::F32(b) => b.append_option(v.and_then(|v| v.as_f64()).map(|x| x as f32)),
            Col::F64(b) => b.append_option(v.and_then(|v| v.as_f64())),
            Col::Str(b) => b.append_option(v.and_then(|v| v.as_str())),
            Col::List(b) => match v {
                Some(v) => {
                    let ok = v.push_f64_list(b.values());
                    b.append(ok)
                }
                None => b.append(false),
            },
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Col::I64(b) => Arc::new(b.finish()),
            Col::U32(b) => Arc::new(b.finish()),
            Col::U8(b) => Arc::new(b.finish()),
            Col::F32(b) => Arc::new(b.finish()),
            Col::F64(b) => Arc::new(b.finish()),
            Col::Str(b) => Arc::new(b.finish()),
            Col::List(b) => Arc::new(b.finish()),
        }
    }
}

pub fn new_cols(decl: &[Field], cap: usize) -> Vec<Col> {
    decl.iter().map(|f| Col::new(f.ty, cap)).collect()
}

pub fn finish(decl: &[Field], mut cols: Vec<Col>) -> RecordBatch {
    let arrays = decl
        .iter()
        .zip(cols.iter_mut())
        .map(|(f, c)| (f.name, c.finish()));
    let batch = RecordBatch::try_from_iter(arrays).expect("record batch");
    debug_assert_eq!(batch.num_columns(), decl.len());
    debug_assert!(batch.column(0).len() == batch.num_rows());
    batch
}
