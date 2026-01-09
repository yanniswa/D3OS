/* ╔═════════════════════════════════════════════════════════════════════════╗
   ║ Module: wait_queue                                                      ║
   ╟─────────────────────────────────────────────────────────────────────────╢
   ║ Wait queues for blocking i/o.                                           ║
   ║                                                                         ║
   ║ Public functions:                                                       ║
   ║   - wait:       Blocks calling thread if the given predicate is true.   ║
   ║   - notify_one: Deblocks one waiting thread (if any).                   ║
   ╟─────────────────────────────────────────────────────────────────────────╢
   ║ Author: Michael Schoettner, Univ. Duesseldorf, 30.12.2025               ║
   ╚═════════════════════════════════════════════════════════════════════════╝
*/

use alloc::collections::VecDeque;

use crate::scheduler;
use crate::sync::irqsave_spinlock::IrqSaveSpinlock;

pub struct WaitQueue {
    queue: IrqSaveSpinlock<VecDeque<(usize, usize)>>,
}

impl WaitQueue {
    pub fn new() -> WaitQueue {
        WaitQueue {
            queue: IrqSaveSpinlock::new(VecDeque::<(usize, usize)>::new()),
        }
    }

    /// Block until `pred()` becomes true.
    pub fn wait<F>(&self, mut pred: F)
    where
        F: FnMut() -> bool,
    {
        // Because of spurious wakeups, we need to loop here.
        loop {
            // Check predicate without acquiring the queue lock
            if pred() {
                return;
            }

            // Take the queue lock synchronizing against `notify_one`
            {
                let mut quard = self.queue.lock(); // IRQs disabled & spinlocked here

                if pred() {
                    // Condition became true while we were getting the lock; don't sleep.
                    return;
                }

                // Get caller thread's (pid, tid)
                let (pid, tid) = scheduler().current_ids();

                // Enqueue ourselves as a waiter
                quard.push_back((pid, tid));

                // Mark the caller thread as blocked
                scheduler().prepare_to_block();

                // lock is dropped here => IRQs restored
            }

            // Yield the CPU to other threads. This must be outside the lock because we free cpu here.
            scheduler().block_if_allowed();

            // On wake, loop and check pred() again.
        }
    }

    /// Wake up exactly one waiter (if any). Returns true if someone was woken up.
    pub fn notify_one(&self) -> bool {
        let mut guard = self.queue.lock();

        while let Some((pid, tid)) = guard.pop_front() {
            if scheduler().unblock(pid, tid) {
                return true;
            }
            // else: stale waiter (killed/exited) -> keep going
        }

        false
    }
}
