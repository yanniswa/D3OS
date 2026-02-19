/* ╔═════════════════════════════════════════════════════════════════════════╗
   ║ Module: tmpfs                                                           ║
   ╟─────────────────────────────────────────────────────────────────────────╢
   ║ Temporary file system running storing everything in main memory. It     ║
   ║ supports directories, files, and named pipes.                           ║
   ╟─────────────────────────────────────────────────────────────────────────╢
   ║ Author: Michael Schoettner, Univ. Duesseldorf, 17.1.2026                ║
   ╚═════════════════════════════════════════════════════════════════════════╝
*/
use super::stat::Mode;
use super::stat::Stat;
use super::traits::{DirectoryObject, FileObject, FileSystem, NamedObject, PipeObject};
use crate::process::scheduler;
use crate::scheduler;
use crate::sync::wait_queue::WaitQueue;
use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::fmt::{Debug, Formatter};
use core::result::Result;
use core::sync::atomic::AtomicBool;
use core::sync::atomic::AtomicUsize;
use core::sync::atomic::Ordering;
use core::{fmt, ptr};
use log::{info, warn};
use naming::shared_types::{DirEntry, FileType, OpenOptions};
use nolock::queues::mpmc;
use spin::rwlock::RwLock;
use syscall::return_vals::Errno;

pub struct TmpFs {
    root_dir: Arc<Dir>,
}

impl TmpFs {
    pub fn new() -> TmpFs {
        TmpFs {
            root_dir: Arc::new(Dir::new()),
        }
    }

    pub fn create_static_file(&self, path: &str, buffer: &'static [u8]) -> Result<NamedObject, Errno> {
        let mut dir = self.root_dir.as_ref();

        let (path, filename) = match path.rsplit_once("/") {
            None => ("", path),
            Some((path, name)) => (path, name),
        };

        for component in path.split("/").filter(|s| !s.is_empty()) {
            let name = component.to_string();
            let new_dir = match dir.lookup(component) {
                Ok(new_dir) => new_dir,
                Err(Errno::ENOENT) => dir.create_dir(name.as_str(), Mode::new(0)).expect("Failed to create directory"),
                Err(_) => panic!("Failed to lookup or create directory: {}", component),
            };

            dir = unsafe { (ptr::from_ref(new_dir.as_dir()?.as_ref()) as *const Dir).as_ref().unwrap() };
        }

        dir.create_static_file(filename, buffer)
    }
}

impl FileSystem for TmpFs {
    fn root_dir(&self) -> Arc<dyn DirectoryObject> {
        self.root_dir.clone()
    }
}

enum TmpFsINode {
    File(Arc<dyn FileObject>),
    Pipe(Arc<dyn PipeObject>),
    Directory(Arc<Dir>),
}

struct DirInner {
    files: Vec<(String, TmpFsINode)>,
    stat: Stat,
}

pub struct Dir(RwLock<DirInner>);

impl Dir {
    pub fn new() -> Dir {
        Dir(RwLock::new(DirInner {
            files: Vec::new(),
            stat: Stat {
                mode: Mode::new(0),
                ..Stat::zeroed()
            },
        }))
    }

    pub fn create_static_file(&self, name: &str, buffer: &'static [u8]) -> Result<NamedObject, Errno> {
        let mut dir_lock = self.0.write();

        // Check if the file already exists in the directory
        if dir_lock.files.iter().any(|(file_name, _)| file_name == name) {
            return Err(Errno::EEXIST); // Return an error if the file exists
        }

        // Create a new file and add it to the directory
        let inode = Arc::new(StaticFile::new(buffer));
        dir_lock.files.push((name.to_string(), TmpFsINode::File(inode.clone())));

        // Return the created file as a NamedObject
        Ok((inode as Arc<dyn FileObject>).into())
    }
}

impl DirectoryObject for Dir {
    // check if an object with the given name exists in the directory
    fn lookup(&self, name: &str) -> Result<NamedObject, Errno> {
        let guard = self.0.read(); // Lock the mutex to access the inner data
        if let Some((_, tmpfs_inode)) = guard.files.iter().find(|(file_name, _)| file_name == name) {
            // Match on the TmpFsINode type
            match tmpfs_inode {
                TmpFsINode::File(file) => Ok(file.clone().into()), // Clone and convert to NamedObject
                TmpFsINode::Pipe(pipe) => Ok(pipe.clone().into()), // Clone and convert to NamedObject
                TmpFsINode::Directory(dir) => Ok((dir.clone() as Arc<dyn DirectoryObject>).into()), // Clone and cast directory
            }
        } else {
            Err(Errno::ENOENT) // Return error if the file is not found
        }
    }

    fn create_pipe(&self, name: &str, _mode: Mode) -> Result<NamedObject, Errno> {
        let mut dir_lock = self.0.write();

        // Check if the pipe already exists in the directory
        if dir_lock.files.iter().any(|(file_name, _)| file_name == name) {
            return Err(Errno::EEXIST); // Return an error if the file exists
        }

        // Create a new pipe and add it to the directory
        let inode = Arc::new(Pipe::new());
        dir_lock.files.push((name.to_string(), TmpFsINode::Pipe(inode.clone())));

        // Return the created file as a NamedObject
        Ok((inode as Arc<dyn PipeObject>).into())
    }

    fn create_file(&self, name: &str, _mode: Mode) -> Result<NamedObject, Errno> {
        let mut dir_lock = self.0.write();

        // Check if the file already exists in the directory
        if dir_lock.files.iter().any(|(file_name, _)| file_name == name) {
            return Err(Errno::EEXIST); // Return an error if the file exists
        }

        // Create a new file and add it to the directory
        let inode = Arc::new(File::new());
        dir_lock.files.push((name.to_string(), TmpFsINode::File(inode.clone())));

        // Return the created file as a NamedObject
        Ok((inode as Arc<dyn FileObject>).into())
    }

    fn create_dir(&self, name: &str, _mode: Mode) -> Result<NamedObject, Errno> {
        let mut dir_lock = self.0.write();

        // Check if a file or directory with the same name already exists
        if dir_lock.files.iter().any(|(file_name, _)| file_name == name) {
            return Err(Errno::EEXIST); // Return an error if the name exists
        }

        // Create a new directory and add it to the directory's entries
        let inode = Arc::new(Dir::new());
        dir_lock.files.push((name.to_string(), TmpFsINode::Directory(inode.clone())));

        // Return the created directory as a NamedObject
        Ok((inode as Arc<dyn DirectoryObject>).into())
    }

    fn stat(&self) -> Result<Stat, Errno> {
        Ok(self.0.read().stat)
    }

    fn readdir(&self, index: usize) -> Result<Option<DirEntry>, Errno> {
        let dir_lock = self.0.read();
        let (name, inode) = match dir_lock.files.get(index) {
            Some(entry) => entry,
            None => {
                return Ok(None);
            }
        };

        let entry = match inode {
            TmpFsINode::Directory(_dir) => DirEntry {
                file_type: FileType::Directory,
                name: name.clone(),
            },
            TmpFsINode::File(_file) => DirEntry {
                file_type: FileType::Regular,
                name: name.clone(),
            },
            TmpFsINode::Pipe(_pipe) => DirEntry {
                file_type: FileType::NamedPipe,
                name: name.clone(),
            },
        };
        Ok(Some(entry))
    }
}

impl fmt::Debug for Dir {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TmpFsDir").finish()
    }
}

struct File {
    data: RwLock<Vec<u8>>,
    stat: RwLock<Stat>,
}

impl File {
    pub fn new() -> File {
        File {
            data: RwLock::new(Vec::new()),
            stat: RwLock::new(Stat {
                mode: Mode::new(0),
                ..Stat::zeroed()
            }),
        }
    }
}

impl FileObject for File {
    fn stat(&self) -> Result<Stat, Errno> {
        Ok(*self.stat.read())
    }

    fn read(&self, buf: &mut [u8], offset: usize, _options: OpenOptions) -> Result<usize, Errno> {
        let data = self.data.write();
        if offset > data.len() {
            return Ok(0);
        }

        let len = if data.len() - offset < buf.len() { data.len() - offset } else { buf.len() };

        buf[0..len].clone_from_slice(&data[offset..offset + len]);
        Ok(len)
    }

    fn write(&self, buf: &[u8], offset: usize, _options: OpenOptions) -> Result<usize, Errno> {
        let mut data = self.data.write();

        if offset + buf.len() > data.len() {
            let mut stat = self.stat.write();
            stat.size = offset + buf.len();

            data.resize(stat.size, 0);
        }

        data[offset..offset + buf.len()].clone_from_slice(buf);
        Ok(buf.len())
    }
}

impl Debug for File {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TmpFsFile").finish()
    }
}

struct StaticFile {
    data: &'static [u8],
    stat: Stat,
}

impl StaticFile {
    pub fn new(data: &'static [u8]) -> StaticFile {
        StaticFile {
            data,
            stat: Stat {
                size: data.len(),
                ..Stat::zeroed()
            },
        }
    }
}

impl FileObject for StaticFile {
    fn stat(&self) -> Result<Stat, Errno> {
        Ok(self.stat)
    }

    fn read(&self, buf: &mut [u8], offset: usize, _options: OpenOptions) -> Result<usize, Errno> {
        if offset > self.data.len() {
            return Ok(0);
        }

        let len = if self.data.len() - offset < buf.len() {
            self.data.len() - offset
        } else {
            buf.len()
        };

        buf[0..len].clone_from_slice(&self.data[offset..offset + len]);
        Ok(len)
    }

    fn write(&self, _buf: &[u8], _offset: usize, _options: OpenOptions) -> Result<usize, Errno> {
        Err(Errno::ERDONLY)
    }
}

impl Debug for StaticFile {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TmpFsStaticFile").finish()
    }
}

const PIPE_SIZE: usize = 0x1000;

struct PipeQueue {
    rx: mpmc::bounded::scq::Receiver<u8>,
    wx: mpmc::bounded::scq::Sender<u8>,
}

struct Pipe {
    stat: RwLock<Stat>,
    pq: RwLock<PipeQueue>,
    rx_wq: WaitQueue,       // readers block when pipe is empty
    wx_wq: WaitQueue,       // writers block when pipe is full
    count: AtomicUsize,     // number of bytes currently in the pipe
    has_reader: AtomicBool, // true if opened for reading
    has_writer: AtomicBool, // true if opened for writing
    mutex: spin::Mutex<()>, // protects critical sections in open/closer
}

impl Pipe {
    pub fn new() -> Pipe {
        let (rx, wx) = mpmc::bounded::scq::queue(PIPE_SIZE);
        Self {
            stat: RwLock::new(Stat {
                mode: Mode::new(0),
                ..Stat::zeroed()
            }),
            pq: RwLock::new(PipeQueue { rx, wx }),
            rx_wq: WaitQueue::new(),
            wx_wq: WaitQueue::new(),
            count: AtomicUsize::new(0),
            has_reader: AtomicBool::new(false),
            has_writer: AtomicBool::new(false),
            mutex: spin::Mutex::new(()),
        }
    }

    #[inline]
    fn has_data(&self) -> bool {
        self.count.load(Ordering::SeqCst) > 0
    }

    #[inline]
    fn has_space(&self) -> bool {
        self.count.load(Ordering::SeqCst) < PIPE_SIZE
    }

    #[inline]
    fn has_reader(&self) -> bool {
        self.has_reader.load(Ordering::SeqCst)
    }

    #[inline]
    fn has_writer(&self) -> bool {
        self.has_writer.load(Ordering::SeqCst)
    }
}

impl PipeObject for Pipe {

    fn open(&self, flags: OpenOptions) -> Result<usize, Errno> {
        let guard = self.mutex.lock();
        //let (pid, tid) = scheduler().current_ids();

        match flags {
            OpenOptions::READONLY => {
                //info!("PipeObject::open: READONLY, pid={}, tid={}", pid, tid);
                if self.has_reader() {
                    //info!("PipeObject::open failed -> EBUSY");
                    return Err(Errno::EBUSY);
                }
                self.has_reader.store(true, Ordering::SeqCst);
                
                self.wx_wq.notify_one();

                drop(guard); // release lock before blocking
                self.rx_wq.wait(|| self.has_writer(), "open for reader waiting for writer");
                Ok(0)
            }
            OpenOptions::WRITEONLY => {
                //info!("PipeObject::open: WRITEONLY, pid={}, tid={}", pid, tid);
                if self.has_writer() {
                    //info!("PipeObject::open failed -> EBUSY");
                    return Err(Errno::EBUSY);
                }
                self.has_writer.store(true, Ordering::SeqCst);

                self.rx_wq.notify_one();

                drop(guard); // release lock before blocking
                self.wx_wq.wait(|| self.has_reader(), "open for writer waiting for reader");
                Ok(0)
            }
            _ => Err(Errno::EINVAL),
        }
    }

    fn stat(&self) -> Result<Stat, Errno> {
        Ok(*self.stat.read())
    }

    /// Read from pipe buffer, `offset` is ignored
    fn read(&self, buf: &mut [u8], _offset: usize, options: OpenOptions) -> Result<usize, Errno> {

        // Debug output
        //let (pid, tid) = scheduler().current_ids();
       // info!("read: pid={}, tid={}", pid, tid);

        // check if pipe was opened for reading
        if options == OpenOptions::WRITEONLY {
            return Err(Errno::EBADF);
        }

        // buf has len = 0 ?
        if buf.len() == 0 {
            return Ok(0);
        }

        // Block until data is available or writer has gone
        self.rx_wq.wait(|| self.has_data() || !self.has_writer(), "read: blocks");

        // EOF if no writer is present and no data available
        if !self.has_data() && !self.has_writer() {
            return Ok(0);
        }

        // From here we read data
        // We have data but the writer might have gone or leaves concurrently 

        let total_to_read = buf.len();
        let mut total_read = 0;
        let pq = self.pq.read();
        loop {
            // Are we done?
            if total_read >= total_to_read {
                break;
            }

            // Read one byte
            match pq.rx.try_dequeue() {
                Ok(byte) => {
                    // We consumed a byte
                    self.count.fetch_sub(1, Ordering::SeqCst);
                    buf[total_read] = byte;
                    total_read += 1;
                }
                Err(_) => {

                    // We consumed all available data but need more
                    // We block until more data is available or the writer has gone (-> EOF)
                    self.rx_wq.wait(|| self.has_data() || !self.has_writer(), "read: blocks");
                    if !self.has_data() {
                        break;
                    }
                }
            }
        }

        // If we read at least one byte we freed space 
        // -> wake potentially blocked writer
        if total_read > 0 {
            self.wx_wq.notify_one();
        } 

        Ok(total_read)
    }

    /// Write to pipe buffer, `offset` is ignored
    fn write(&self, buf: &[u8], _offset: usize, options: OpenOptions) -> Result<usize, Errno> {

        // Debug output
        //let (pid, tid) = scheduler().current_ids();
        //info!("write: pid={}, tid={}", pid, tid);

        // check if pipe was opened for reading
        if options == OpenOptions::READONLY {
            return Err(Errno::EBADF);
        }

        // buf has len = 0 ?
        if buf.len() == 0 {
            return Ok(0);
        }

        // Block until space is available or reader has gone
        self.wx_wq.wait(|| self.has_space() || !self.has_reader(), "write: blocks");

        // EOF if no writer is present and no data available
        if !self.has_reader() {
            return Err(Errno::EPIPE);
        }

        // From here we write data
        // We have space but the reader might leave concurrently 
        let total_to_write: usize = buf.len();
        let mut total_written = 0;
        let pq = self.pq.read();
        loop {
            // Are we done?
            if total_written >= total_to_write {
                break;
            }

            // Write one byte
            match pq.wx.try_enqueue(buf[total_written]) {
                Ok(byte) => {
                    // We wrote a byte
                    self.count.fetch_add(1, Ordering::SeqCst);
                    total_written += 1;
                }
                Err(_) => {

                    // We consumed all available space but need more
                    // We block until more space is available or the reader has gone (-> EOF)
                    self.wx_wq.wait(|| self.has_space() || !self.has_reader(), "write: blocks");
                    if !self.has_reader() {
                        return Err(Errno::EPIPE);
                    } 
                }
            }
        }

        // If we wrote at least one byte we wake up potentially blocked reader
        if total_written > 0 {
            //info!("PipeObject::write: done, total_written={}, notify_one, pid={}, tid={}", total_written, pid, tid);
            self.rx_wq.notify_one();
        } 
        Ok(total_written)
    }

    fn close(&self, flags: OpenOptions) {
        let (pid, tid) = scheduler().current_ids();

        let _guard = self.mutex.lock();

        //info!("    pipe close, flags={:?}", flags);
        match flags {
            OpenOptions::READONLY => {
                //info!("PipeObject::close: READONLY, pid={}, tid={}", pid, tid);
                self.has_reader.store(false, Ordering::SeqCst);
                self.wx_wq.notify_all();
            }
            OpenOptions::WRITEONLY => {
                //info!("PipeObject::close: WRITEONLY, pid={}, tid={}", pid, tid);
                self.has_writer.store(false, Ordering::SeqCst);
                self.rx_wq.notify_all();
            }
            _ => (),
        }

        if !self.has_reader() && !self.has_writer() {
           // info!("PipeObject::close: fully closed, pid={}, tid={}", pid, tid);
            let (rx, wx) = mpmc::bounded::scq::queue(PIPE_SIZE);
            let mut pq = self.pq.write();
            pq.rx = rx;
            pq.wx = wx;

            self.count.store(0, Ordering::SeqCst);
        }
    }
}

impl Debug for Pipe {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("NamedPipe").finish()
    }
}
