//! Keeps rust-g loaded on Windows until the process exits.
//!
//! When a world is stopped in Dream Daemon, BYOND unloads rust-g with `FreeLibrary`, but the threads rust-g
//! started keep running: rayon's global pool (iconforge, cellularnoise, worleynoise, cave_system_generator),
//! `jobs`, `http` and `redis_pubsub`. Once the module is unloaded their code is gone, so the next time one of
//! them runs, Dream Daemon crashes with the faulting module `rust_g.dll_unloaded`.
//!
//! Pinning the module when it loads makes `FreeLibrary` leave it in place, so those threads always have code
//! to run. A world started later in the same process gets this same instance back from `LoadLibrary`.

use std::ffi::c_void;

const DLL_PROCESS_ATTACH: u32 = 1;
const GET_MODULE_HANDLE_EX_FLAG_PIN: u32 = 0x1;
const GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS: u32 = 0x4;

#[link(name = "kernel32")]
unsafe extern "system" {
    fn GetModuleHandleExW(flags: u32, module_name: *const u16, module: *mut *mut c_void) -> i32;
}

#[unsafe(no_mangle)]
#[allow(non_snake_case)]
extern "system" fn DllMain(module: *mut c_void, reason: u32, _reserved: *mut c_void) -> i32 {
    if reason == DLL_PROCESS_ATTACH {
        let mut pinned = std::ptr::null_mut();
        // SAFETY: with FROM_ADDRESS, the name argument is an address inside the module, and `module` is this
        // DLL's own base address. Failing to pin only leaves the old behaviour, so the result isn't checked.
        unsafe {
            GetModuleHandleExW(
                GET_MODULE_HANDLE_EX_FLAG_PIN | GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS,
                module as *const u16,
                &mut pinned,
            );
        }
    }
    1
}
