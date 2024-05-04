use rand::distributions::DistString;

/// Buffer is a wrapper for io::Write.
pub struct Buffer<W> {
    pub inner: W,
    pub count: usize,
}

impl<W> Buffer<W>
where
    W: std::io::Write,
{
    pub fn new(inner: W) -> Self {
        Self { inner, count: 0 }
    }
}

impl<W> std::io::Write for Buffer<W>
where
    W: std::io::Write,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = self.inner.write(buf)?;
        self.count += len;
        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

pub fn generate_random_alphanumeric_string(len: usize) -> String {
    rand::distributions::Alphanumeric.sample_string(&mut rand::thread_rng(), len)
}
