use monoio::io::{AsyncReadRent, AsyncWriteRent};
use monoio::io::AsyncWriteRentExt;

pub async fn copy_data<Read: AsyncReadRent, Write: AsyncWriteRent>(
    local: &mut Read,
    remote: &mut Write,
) -> Result<Vec<u8>, std::io::Error> {
    let mut buf = vec![0; 1024];
    
    loop {
        let (res, read_buffer) = local.read(buf).await;
        buf = read_buffer;
        let read_len = res?;
        if read_len == 0 {
            return Ok(buf);
        }
        
        let (res, write_buffer) = remote.write_all(buf).await;
        buf = write_buffer;
        let _ = res?;
        buf.clear();
    }
}
