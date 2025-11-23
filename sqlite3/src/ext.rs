use std::ffi::{c_char, c_void, CStr, CString};
use std::sync::{Mutex, OnceLock};

#[cfg(not(target_family = "wasm"))]
use libloading::Library;

use crate::{sqlite3, SQLITE_ERROR, SQLITE_MISUSE, SQLITE_NOMEM, SQLITE_OK};

/// Number of entries in `sqlite3_api_routines` for the SQLite 3.42 API surface.
pub const SQLITE3_API_ROUTINE_COUNT: usize = 287;

/// Minimal representation of the `sqlite3_api_routines` table required by `sqlite3ext.h`.
#[repr(C)]
pub struct sqlite3_api_routines {
    pub functions: [*const c_void; SQLITE3_API_ROUTINE_COUNT],
}

pub type ExtensionEntryPoint =
    unsafe extern "C" fn(*mut sqlite3, *mut *mut c_char, *const sqlite3_api_routines) -> i32;

static API_ROUTINES: OnceLock<sqlite3_api_routines> = OnceLock::new();
static AUTO_EXTENSIONS: OnceLock<Mutex<Vec<ExtensionEntryPoint>>> = OnceLock::new();

// Subset of sqlite3ext.h indices we currently populate.
mod offsets {
    pub const FREE: usize = 64;
    pub const MALLOC: usize = 74;
    pub const REALLOC: usize = 82;
    pub const LIBVERSION: usize = 72;
    pub const LIBVERSION_NUMBER: usize = 73;
    pub const OPEN: usize = 76;
    pub const OPEN16: usize = 77;
    pub const PREPARE_V2: usize = 122;
    pub const PREPARE16_V2: usize = 123;
    pub const STEP: usize = 100;
    pub const FINALIZE: usize = 63;
    pub const RESET: usize = 83;
    pub const CLOSE: usize = 16;
    pub const CLOSE_V2: usize = 190;
    pub const ERRMSG: usize = 59;
    pub const ERRCODE: usize = 58;
    pub const ERRSTR: usize = 194;
    pub const TOTAL_CHANGES: usize = 103;
    pub const LAST_INSERT_ROWID: usize = 71;
    pub const BIND_INT: usize = 4;
    pub const BIND_INT64: usize = 5;
    pub const BIND_NULL: usize = 6;
    pub const BIND_TEXT: usize = 10;
    pub const BIND_BLOB: usize = 2;
    pub const BIND_PARAMETER_COUNT: usize = 7;
    pub const BIND_PARAMETER_NAME: usize = 9;
    pub const COLUMN_INT: usize = 28;
    pub const COLUMN_INT64: usize = 29;
    pub const COLUMN_TEXT: usize = 36;
    pub const COLUMN_BLOB: usize = 19;
    pub const COLUMN_BYTES: usize = 20;
    pub const COLUMN_COUNT: usize = 22;
    pub const COLUMN_TYPE: usize = 38;
    pub const VALUE_TYPE: usize = 119;
    pub const VALUE_INT64: usize = 113;
    pub const VALUE_DOUBLE: usize = 111;
    pub const VALUE_TEXT: usize = 115;
    pub const VALUE_BLOB: usize = 108;
    pub const VALUE_BYTES: usize = 109;
    pub const MALLOC64: usize = 208;
    pub const REALLOC64: usize = 210;
    pub const LOAD_EXTENSION: usize = 207;
    pub const AUTO_EXTENSION: usize = 203;
    pub const CANCEL_AUTO_EXTENSION: usize = 206;
    pub const RESET_AUTO_EXTENSION: usize = 211;
}

fn set_fn(table: &mut sqlite3_api_routines, idx: usize, func: *const c_void) {
    if idx < SQLITE3_API_ROUTINE_COUNT {
        table.functions[idx] = func;
    }
}

pub fn api_routines() -> &'static sqlite3_api_routines {
    API_ROUTINES.get_or_init(|| {
        let mut table = sqlite3_api_routines {
            functions: [std::ptr::null(); SQLITE3_API_ROUTINE_COUNT],
        };
        unsafe {
            use offsets::*;
            set_fn(&mut table, FREE, crate::sqlite3_free as *const c_void);
            set_fn(&mut table, MALLOC, crate::sqlite3_malloc as *const c_void);
            set_fn(&mut table, MALLOC64, crate::sqlite3_malloc64 as *const c_void);
            set_fn(&mut table, REALLOC, crate::sqlite3_realloc as *const c_void);
            set_fn(&mut table, REALLOC64, crate::sqlite3_realloc64 as *const c_void);
            set_fn(&mut table, LIBVERSION, crate::sqlite3_libversion as *const c_void);
            set_fn(
                &mut table,
                LIBVERSION_NUMBER,
                crate::sqlite3_libversion_number as *const c_void,
            );
            set_fn(&mut table, OPEN, crate::sqlite3_open as *const c_void);
            set_fn(&mut table, OPEN16, crate::sqlite3_open16 as *const c_void);
            set_fn(&mut table, CLOSE, crate::sqlite3_close as *const c_void);
            set_fn(&mut table, CLOSE_V2, crate::sqlite3_close_v2 as *const c_void);
            set_fn(&mut table, PREPARE_V2, crate::sqlite3_prepare_v2 as *const c_void);
            set_fn(
                &mut table,
                PREPARE16_V2,
                crate::sqlite3_prepare16_v2 as *const c_void,
            );
            set_fn(&mut table, STEP, crate::sqlite3_step as *const c_void);
            set_fn(&mut table, FINALIZE, crate::sqlite3_finalize as *const c_void);
            set_fn(&mut table, RESET, crate::sqlite3_reset as *const c_void);
            set_fn(&mut table, ERRMSG, crate::sqlite3_errmsg as *const c_void);
            set_fn(&mut table, ERRCODE, crate::sqlite3_errcode as *const c_void);
            set_fn(&mut table, ERRSTR, crate::sqlite3_errstr as *const c_void);
            set_fn(
                &mut table,
                TOTAL_CHANGES,
                crate::sqlite3_total_changes as *const c_void,
            );
            set_fn(
                &mut table,
                LAST_INSERT_ROWID,
                crate::sqlite3_last_insert_rowid as *const c_void,
            );
            set_fn(&mut table, BIND_INT, crate::sqlite3_bind_int as *const c_void);
            set_fn(&mut table, BIND_INT64, crate::sqlite3_bind_int64 as *const c_void);
            set_fn(&mut table, BIND_NULL, crate::sqlite3_bind_null as *const c_void);
            set_fn(&mut table, BIND_TEXT, crate::sqlite3_bind_text as *const c_void);
            set_fn(&mut table, BIND_BLOB, crate::sqlite3_bind_blob as *const c_void);
            set_fn(
                &mut table,
                BIND_PARAMETER_COUNT,
                crate::sqlite3_bind_parameter_count as *const c_void,
            );
            set_fn(
                &mut table,
                BIND_PARAMETER_NAME,
                crate::sqlite3_bind_parameter_name as *const c_void,
            );
            set_fn(&mut table, COLUMN_INT, crate::sqlite3_column_int as *const c_void);
            set_fn(
                &mut table,
                COLUMN_INT64,
                crate::sqlite3_column_int64 as *const c_void,
            );
            set_fn(&mut table, COLUMN_TEXT, crate::sqlite3_column_text as *const c_void);
            set_fn(&mut table, COLUMN_BLOB, crate::sqlite3_column_blob as *const c_void);
            set_fn(
                &mut table,
                COLUMN_BYTES,
                crate::sqlite3_column_bytes as *const c_void,
            );
            set_fn(
                &mut table,
                COLUMN_COUNT,
                crate::sqlite3_column_count as *const c_void,
            );
            set_fn(
                &mut table,
                COLUMN_TYPE,
                crate::sqlite3_column_type as *const c_void,
            );
            set_fn(&mut table, VALUE_TYPE, crate::sqlite3_value_type as *const c_void);
            set_fn(
                &mut table,
                VALUE_INT64,
                crate::sqlite3_value_int64 as *const c_void,
            );
            set_fn(
                &mut table,
                VALUE_DOUBLE,
                crate::sqlite3_value_double as *const c_void,
            );
            set_fn(&mut table, VALUE_TEXT, crate::sqlite3_value_text as *const c_void);
            set_fn(&mut table, VALUE_BLOB, crate::sqlite3_value_blob as *const c_void);
            set_fn(
                &mut table,
                VALUE_BYTES,
                crate::sqlite3_value_bytes as *const c_void,
            );
            set_fn(
                &mut table,
                LOAD_EXTENSION,
                crate::sqlite3_load_extension as *const c_void,
            );
            set_fn(
                &mut table,
                AUTO_EXTENSION,
                crate::sqlite3_auto_extension as *const c_void,
            );
            set_fn(
                &mut table,
                CANCEL_AUTO_EXTENSION,
                crate::sqlite3_cancel_auto_extension as *const c_void,
            );
            set_fn(
                &mut table,
                RESET_AUTO_EXTENSION,
                crate::sqlite3_reset_auto_extension as *const c_void,
            );
        }
        table
    })
}

pub fn register_auto_extension(entry: ExtensionEntryPoint) -> i32 {
    let list = AUTO_EXTENSIONS.get_or_init(|| Mutex::new(Vec::new()));
    let mut guard = match list.lock() {
        Ok(g) => g,
        Err(_) => return SQLITE_NOMEM,
    };
    if guard
        .iter()
        .any(|&existing| existing as usize == entry as usize)
    {
        return SQLITE_OK;
    }
    guard.push(entry);
    SQLITE_OK
}

pub fn cancel_auto_extension(entry: ExtensionEntryPoint) -> i32 {
    let list = AUTO_EXTENSIONS.get_or_init(|| Mutex::new(Vec::new()));
    let mut guard = match list.lock() {
        Ok(g) => g,
        Err(_) => return SQLITE_NOMEM,
    };
    let before = guard.len();
    guard.retain(|&existing| existing as usize != entry as usize);
    if guard.len() != before {
        1
    } else {
        0
    }
}

pub fn reset_auto_extensions() {
    if let Some(list) = AUTO_EXTENSIONS.get() {
        if let Ok(mut guard) = list.lock() {
            guard.clear();
        }
    }
}

pub fn invoke_auto_extensions(db: *mut sqlite3) -> i32 {
    let list = AUTO_EXTENSIONS.get_or_init(|| Mutex::new(Vec::new()));
    let guard = match list.lock() {
        Ok(g) => g,
        Err(_) => return SQLITE_NOMEM,
    };
    let api = api_routines() as *const sqlite3_api_routines;
    for entry in guard.iter().copied() {
        let mut err: *mut c_char = std::ptr::null_mut();
        let rc = unsafe { entry(db, &mut err, api) };
        if rc != SQLITE_OK {
            // The caller is responsible for propagating the error message from the extension if needed.
            return rc;
        }
    }
    SQLITE_OK
}

#[cfg(not(target_family = "wasm"))]
pub unsafe fn load_dynamic_extension(
    db: *mut sqlite3,
    path: &std::path::Path,
    entrypoint: &CStr,
    err_out: *mut *mut c_char,
    keep_alive: &mut Vec<Library>,
) -> i32 {
    let lib = match Library::new(path) {
        Ok(lib) => lib,
        Err(err) => return set_error(err_out, format!("failed to load extension: {err}")),
    };

    let symbol: Result<libloading::Symbol<ExtensionEntryPoint>, _> =
        lib.get(entrypoint.to_bytes_with_nul());
    let entry = match symbol {
        Ok(entry) => entry,
        Err(err) => return set_error(err_out, format!("failed to find entry point: {err}")),
    };

    let rc = entry(db, err_out, api_routines() as *const _);
    if rc == SQLITE_OK {
        keep_alive.push(lib);
    }
    rc
}

#[cfg(target_family = "wasm")]
#[allow(unused_variables)]
pub unsafe fn load_dynamic_extension(
    _db: *mut sqlite3,
    _path: &std::path::Path,
    _entrypoint: &CStr,
    _err_out: *mut *mut c_char,
    _keep_alive: &mut Vec<()>,
) -> i32 {
    SQLITE_MISUSE
}

fn set_error(dest: *mut *mut c_char, msg: String) -> i32 {
    if !dest.is_null() {
        if let Ok(cstr) = CString::new(msg) {
            unsafe { *dest = cstr.into_raw() };
        }
    }
    SQLITE_ERROR
}
