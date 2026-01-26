/* ╔═════════════════════════════════════════════════════════════════════════╗
   ║ Module: scheduler                                                       ║
   ╟─────────────────────────────────────────────────────────────────────────╢
   ║ Implementation of a basic round-robin scheduler.                        ║
   ║                                                                         ║
   ║ Public functions                                                        ║
   ║   - active_thread_ids      get a list of all active thread IDs          ║
   ║   - current_thread         get the currently running thread             ║
   ║   - current_ids            get the (pid, tid) of the current thread     ║
   ║   - exit                   exit the calling thread                      ║
   ║   - join                   wait for a thread to finish                  ║
   ║   - kill                   kill a thread                                ║
   ║   - set_init               set the scheduler as initialized             ║
   ║   - thread                 get reference to a thread                    ║
   ║   - ready                  insert a thread in the ready queue           ║
   ║   - sleep                  put the caller into sleeping mode            ║
   ║   - start                  start the scheduler                          ║
   ║   - switch_thread_from_interrupt  switch thread, called from interrupt  ║
   ║   - switch_thread_no_interrupt    switch thread, not called from int.   ║
   ║   - current_ids            get the (pid, tid) of the current thread     ║
   ║   - prepare_to_block       prepare the calling thread to block          ║
   ║   - block_if_allowed       block the calling thread (if ok)             ║
   ║   - unblock                unblock a given thread                       ║
   ║   - get_status             for ps command - get all processes & threads ║
   ╟─────────────────────────────────────────────────────────────────────────╢
   ║ Author: Fabian Ruhland & Michael Schopettner, 04.01.2026, HHU           ║
   ╚═════════════════════════════════════════════════════════════════════════╝
*/
use crate::process::thread::{Thread, ThreadState};
use crate::{allocator, apic, scheduler, timer, tss};
use alloc::collections::VecDeque;
use alloc::format;
use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::fmt::Write;
use core::sync::atomic::AtomicUsize;
use core::sync::atomic::Ordering::Relaxed;
use core::{panic, ptr};
use log::info;
use smallmap::Map;
use spin::{Mutex, MutexGuard};
use syscall::return_vals::Errno;

// thread IDs
static THREAD_ID_COUNTER: AtomicUsize = AtomicUsize::new(1);

pub fn next_thread_id() -> usize {
    THREAD_ID_COUNTER.fetch_add(1, Relaxed)
}

/// Everything related to the threads in ready state in the scheduler
struct ReadyState {
    initialized: bool,
    current_thread: Option<Arc<Thread>>,
    ready_queue: VecDeque<Arc<Thread>>,
}

impl ReadyState {
    pub fn new() -> Self {
        Self {
            initialized: false,
            current_thread: None,
            ready_queue: VecDeque::new(),
        }
    }
}

/// Main struct of the scheduler
pub struct Scheduler {
    ready_state: Mutex<ReadyState>,
    sleep_list: Mutex<Vec<(Arc<Thread>, usize)>>,
    blocked_list: Mutex<Vec<Arc<Thread>>>,
    join_map: Mutex<Map<usize, Vec<Arc<Thread>>>>, // manage which threads are waiting for a thread-id to terminate
}

unsafe impl Send for Scheduler {}
unsafe impl Sync for Scheduler {}

/// Called from assembly code, after the thread has been switched
#[unsafe(no_mangle)]
pub unsafe extern "C" fn unlock_scheduler() {
    unsafe {
        scheduler().ready_state.force_unlock();
    }
}

impl Scheduler {
    /// Create and initialize the scheduler.
    pub fn new() -> Self {
        Self {
            ready_state: Mutex::new(ReadyState::new()),
            sleep_list: Mutex::new(Vec::new()),
            blocked_list: Mutex::new(Vec::new()),
            join_map: Mutex::new(Map::new()),
        }
    }

    /// Called after the scheduler has been fully initialized
    pub fn set_init(&self) {
        self.get_ready_state().initialized = true;
    }

    /// Get all active thread IDs
    pub fn active_thread_ids(&self) -> Vec<usize> {
        let state = self.get_ready_state();
        let sleep_list = self.sleep_list.lock();

        state
            .ready_queue
            .iter()
            .map(|thread| thread.id())
            .collect::<Vec<usize>>()
            .into_iter()
            .chain(sleep_list.iter().map(|entry| entry.0.id()))
            .collect()
    }

    /// Return reference to current thread
    pub fn current_thread(&self) -> Arc<Thread> {
        let state = self.get_ready_state();
        Scheduler::current(&state)
    }

    /// Return reference to thread identified by `thread_id`
    pub fn thread(&self, thread_id: usize) -> Option<Arc<Thread>> {
        self.ready_state.lock().ready_queue.iter().find(|thread| thread.id() == thread_id).cloned()
    }

    /// Return (pid, tid) of current thread
    pub fn current_ids(&self) -> (usize, usize) {
        let tid = self.current_thread().id();
        let pid = self.current_thread().process().id();
        (pid, tid)
    }

    /// Start the scheduler, called only once from `boot.rs`
    pub fn start(&self) {
        // TODO: make sure this is actually called just once
        let mut state = self.get_ready_state();
        state.current_thread = state.ready_queue.pop_back();

        unsafe {
            Thread::start_first(state.current_thread.as_ref().expect("Failed to dequeue first thread!").as_ref());
        }
    }

    /// Insert `thread` into the ready queue of the scheduler
    pub fn ready(&self, thread: Arc<Thread>) {
        let id = thread.id();

        thread.set_state(ThreadState::Ready);
        // If we get the lock on 'self.state' but not on 'self.join_map' the system hangs.
        // The scheduler is not able to switch threads anymore, because of 'self.state' is locked,
        // and we will never be able to get the lock on 'self.join_map'.
        // To solve this, we need to release the lock on 'self.state' in case we do not get
        // the lock on 'self.join_map' and let the scheduler switch threads until we get both locks.
        let (mut state, mut join_map) = loop {
            let state = self.get_ready_state();
            if let Some(join_map) = self.join_map.try_lock() {
                break (state, join_map);
            }
            self.switch_thread_no_interrupt();
        };

        state.ready_queue.push_front(thread);
        join_map.insert(id, Vec::new());
    }

    /// Put calling thread to sleep for `ms` milliseconds
    pub fn sleep(&self, ms: usize) {
        let mut state = self.get_ready_state();

        if !state.initialized {
            // Scheduler is not initialized yet, so this function has been called during the boot process
            // So we do active waiting
            timer().wait(ms);
        } else {
            // Scheduler is initialized, so we can block the calling thread
            let thread = Scheduler::current(&state);
            thread.set_state(ThreadState::Sleeping);
            let wakeup_time = timer().systime_ms() + ms;

            {
                // Execute in own block, so that the lock is released automatically (block() does not return)
                let mut sleep_list = self.sleep_list.lock();
                sleep_list.push((thread, wakeup_time));
            }

            self.block_and_switch(&mut state);
        }
    }

    /// Prepare to block the calling thread
    pub fn prepare_to_block(&self) {
        let state = self.get_ready_state();
        let thread = Scheduler::current(&state);
        thread.reset_wake_pending();
    }

    /// Block calling thread only if allowed; otherwise consume pending wake and return.
    pub fn block_if_allowed(&self) {
        let mut state = self.get_ready_state();

        if !state.initialized {
            panic!("Scheduler: Cannot block thread before scheduler is initialized!");
        }

        let thread = Scheduler::current(&state);

        // If a wake happened "early", don't block.
        if thread.should_block_or_consume_wake() == false {
            // A wake was pending and is now consumed.
            // Thread continues running.
            return;
        }

        // Actually block.
        thread.set_state(ThreadState::Blocked);
        {
            let mut block_list = self.blocked_list.lock();
            block_list.push(thread);
        }
        self.block_and_switch(&mut state);
    }

    /// Unblock thread with given (pid, tid). \
    /// Returns true if thread was found and unblocked, false otherwise.
    pub fn unblock(&self, pid: usize, tid: usize) -> bool {
        // 1) Check if the given thread is in the blocked list -> need to be woken up
        let blocked_thread: Option<Arc<Thread>> = {
            let mut block_list = self.blocked_list.lock();
            if let Some(pos) = block_list.iter().position(|t| t.id() == tid && t.process().id() == pid) {
                Some(block_list.remove(pos))
            } else {
                None
            }
        };

        // If found, wake it up
        if let Some(thread) = blocked_thread {
            let mut state = self.get_ready_state();
            thread.set_state(ThreadState::Ready);
            state.ready_queue.push_front(Arc::clone(&thread));
        }


        /*if let Some(thread) = blocked_thread {
            // Record wake (harmless / consistent with semantics)
            thread.set_state(ThreadState::Ready);
            self.ready(thread);
            return true;
        }*/

        // 2) Check ready queue (thread has not yet blocked) and current thread (thread has not blocked yet and is interrupted from a device interrupt)
        {
            let state = self.get_ready_state();

            // 2a) Current thread (single-core): prevent it from blocking if it's about to.
            if let Some(curr_thread) = &state.current_thread {
                if curr_thread.id() == tid && curr_thread.process().id() == pid {
                    curr_thread.set_wake_pending();
                    return true;
                }
            }
            // 2b) Ready queue
            if let Some(thread) = state.ready_queue.iter().find(|t| t.id() == tid && t.process().id() == pid) {
                thread.set_wake_pending();
                return true;
            }
            // drop(state) here
        }

        // 3) Thread not found in any known list.
        false
    }

    /// Switch from current to next thread (from ready queue). \
    /// If `interrupt` is true, the function is called from an ISR and will send EOI to APIC otherwise not.
    fn switch_thread(&self, interrupt: bool) {
        if let Some(mut state) = self.ready_state.try_lock() {
            if !state.initialized {
                return;
            }

            if let Some(mut sleep_list) = self.sleep_list.try_lock() {
                Scheduler::check_sleep_list(&mut state, &mut sleep_list);
            }

            // Get clone of the current thread
            let current = Scheduler::current(&state);

            // Current thread is initializing itself and may not be interrupted
            if current.stacks_locked() || tss().is_locked() {
                return;
            }

            // Try to get the next thread from the ready queue
            let next = match state.ready_queue.pop_back() {
                Some(thread) => thread,
                None => return,
            };

            let current_ptr = ptr::from_ref(current.as_ref());
            let next_ptr = ptr::from_ref(next.as_ref());

            state.current_thread = Some(next);
            state.ready_queue.push_front(current);

            if interrupt {
                apic().end_of_interrupt();
            }

            unsafe {
                Thread::switch(current_ptr, next_ptr);
            }
        }
    }

    /// Helper function for switching a thread not caused by an interrupt
    pub fn switch_thread_no_interrupt(&self) {
        self.switch_thread(false);
    }

    /// Helper function for switching a thread caused by an interrupt
    pub fn switch_thread_from_interrupt(&self) {
        self.switch_thread(true);
    }

    /// Calling thread will block until thread with `thread_id` has terminated
    pub fn join(&self, thread_id: usize)  -> Result<usize, Errno> {
        let mut state = self.get_ready_state();
        let thread = Scheduler::current(&state);

        {
            // Execute in own block, so that the lock is released automatically (block() does not return)
            let mut join_map = self.join_map.lock();
            if let Some(join_list) = join_map.get_mut(&thread_id) {
                join_list.push(thread);
            } else {
                // Joining on a non-existent thread has no effect (i.e. the thread has already finished running)
                return Err(Errno::ESRCH);
            }
        }

        self.block_and_switch(&mut state);
        Ok(0)
    }

    /// Exit calling thread.
    pub fn exit(&self) -> ! {
        let mut ready_state;
        let current;

       // info!("Scheduler: Exiting thread PID={}, TID={}", self.current_thread().process().id(), self.current_thread().id());
        {
            // Execute in own block, so that join_map is released automatically (block() does not return)
            let state = self.get_ready_state_and_join_map();
            ready_state = state.0;
            let mut join_map = state.1;

            current = Scheduler::current(&ready_state);
            current.set_state(ThreadState::Exited);

         //   info!("Scheduler: searching join-list");
            let join_list = join_map.get_mut(&current.id()).expect("Missing join_map entry!");

            for thread in join_list {
                ready_state.ready_queue.push_front(Arc::clone(thread));
            }

            join_map.remove(&current.id());
        }
       
        drop(current); // Decrease Rc manually, because block() does not return
        self.block_and_switch(&mut ready_state);
        unreachable!()
    }

    /// Kill the thread with the id `thread_id`.
    pub fn kill(&self, thread_id: usize) {
        {
            // Check if current thread tries to kill itself (illegal)
            let ready_state = self.get_ready_state();
            let current = Scheduler::current(&ready_state);

            if current.id() == thread_id {
                panic!("A thread cannot kill itself!");
            }
        }

        let state = self.get_ready_state_and_join_map();
        let mut ready_state = state.0;
        let mut join_map = state.1;

        let join_list = join_map.get_mut(&thread_id).expect("Missing join map entry!");

        for thread in join_list {
            ready_state.ready_queue.push_front(Arc::clone(thread));
        }

        join_map.remove(&thread_id);
        ready_state.ready_queue.retain(|thread| thread.id() != thread_id);
    }

    /// Block calling thread and switch to next ready thread.
    fn block_and_switch(&self, state: &mut ReadyState) {
        let mut next_thread = state.ready_queue.pop_back();

        {
            // Execute in own block, so that the lock is released automatically (block() does not return)
            let mut sleep_list = self.sleep_list.lock();
            while next_thread.is_none() {
                Scheduler::check_sleep_list(state, &mut sleep_list);
                next_thread = state.ready_queue.pop_back();
            }
        }

        let current = Scheduler::current(state);
        let next = next_thread.unwrap();

        // Thread has enqueued itself into sleep list and waited so long, that it dequeued itself in the meantime
        if current.id() == next.id() {
            return;
        }

        let current_ptr = ptr::from_ref(current.as_ref());
        let next_ptr = ptr::from_ref(next.as_ref());

        state.current_thread = Some(next);
        drop(current); // Decrease Rc manually, because Thread::switch does not return

        unsafe {
            Thread::switch(current_ptr, next_ptr);
        }
    }

    /// Return current running thread
    fn current(state: &ReadyState) -> Arc<Thread> {
        Arc::clone(state.current_thread.as_ref().expect("Trying to access current thread before initialization!"))
    }

    /// Check sleep list for threads that need to be waken up
    fn check_sleep_list(state: &mut ReadyState, sleep_list: &mut Vec<(Arc<Thread>, usize)>) {
        let time = timer().systime_ms();

        sleep_list.retain(|entry| {
            if time >= entry.1 {
                entry.0.set_state(ThreadState::Ready);
                state.ready_queue.push_front(Arc::clone(&entry.0));
                false
            } else {
                true
            }
        });
    }

    /// Helper function returning `ReadyState` of scheduler in a MutexGuard
    fn get_ready_state(&self) -> MutexGuard<'_, ReadyState> {
        let state;

        // We need to make sure, that both the kernel memory manager and the ready queue are currently not locked.
        // Otherwise, a deadlock may occur: Since we are holding the ready queue lock,
        // the scheduler won't switch threads anymore, and none of the locks will ever be released
        loop {
            let state_tmp = self.ready_state.lock();
            if allocator().is_locked() {
                continue;
            }

            state = state_tmp;
            break;
        }

        state
    }

    /// Helper function returning `ReadyState` and `Map` of scheduler, each in a MutexGuard
    fn get_ready_state_and_join_map(&self) -> (MutexGuard<'_, ReadyState>, MutexGuard<'_, Map<usize, Vec<Arc<Thread>>>>) {
        loop {
            let ready_state = self.get_ready_state();
            if let Some(join_map) = self.join_map.try_lock() {
                return (ready_state, join_map);
            } else {
                self.switch_thread_no_interrupt();
            }
        }
    }

    /// For ps command - get all processes & threads
    pub fn get_status(&self, buffer: &mut [u8]) -> Result<usize, Errno> {
        let mut out = String::new();

        // Current
        let cur = self.current_thread();
        let _ = writeln!(out, "PID: {}, TID: {}, State: {:?}", cur.process().id(), cur.id(), ThreadState::Running);

        // Ready Queue
        let state = self.get_ready_state();
        for thread in state.ready_queue.iter() {
            let _ = writeln!(out, "PID: {}, TID: {}, State: {:?}", thread.process().id(), thread.id(), thread.state());
        }

        // Sleep List
        let sleep_list = self.sleep_list.lock();
        for entry in sleep_list.iter() {
            // You used thread.0 in dump(), so keep that shape
            let t = &entry.0;
            let _ = writeln!(out, "PID: {}, TID: {}, State: {:?}", t.process().id(), t.id(), t.state());
        }
        drop(sleep_list);

        // Block list
        let block_list = self.blocked_list.lock();
        for thread in block_list.iter() {
            let _ = writeln!(out, "PID: {}, TID: {}, State: {:?}", thread.process().id(), thread.id(), thread.state());
        }
        drop(block_list);

        // Copy to caller buffer (truncate if needed)
        let bytes = out.as_bytes();
        let len = core::cmp::min(bytes.len(), buffer.len());
        buffer[..len].copy_from_slice(&bytes[..len]);
        Ok(len)
    }
}
