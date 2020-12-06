use std::{
    cmp,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{io, ready, AsyncRead, AsyncWrite};
use pin_project::pin_project;
use std::io::{Read, Write};

use super::{TReadTransport, TReadTransportFactory, TWriteTransport, TWriteTransportFactory};

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

/// Default capacity of the read buffer in bytes.
const READ_CAPACITY: usize = 4096;

/// Default capacity of the write buffer in bytes..
const WRITE_CAPACITY: usize = 4096;

#[pin_project]
#[derive(Debug)]
pub struct TBufferedReadTransport<C>
where
    C: AsyncRead,
{
    buf: Box<[u8]>,
    pos: usize,
    cap: usize,
    #[pin]
    chan: C,
}

impl<C> TBufferedReadTransport<C>
where
    C: AsyncRead,
{
    /// Create a `TBufferedTransport` with default-sized internal read and
    /// write buffers that wraps the given `TIoChannel`.
    pub fn new(channel: C) -> TBufferedReadTransport<C> {
        TBufferedReadTransport::with_capacity(READ_CAPACITY, channel)
    }

    /// Create a `TBufferedTransport` with an internal read buffer of size
    /// `read_capacity` and an internal write buffer of size
    /// `write_capacity` that wraps the given `TIoChannel`.
    pub fn with_capacity(read_capacity: usize, channel: C) -> TBufferedReadTransport<C> {
        TBufferedReadTransport {
            buf: vec![0; read_capacity].into_boxed_slice(),
            pos: 0,
            cap: 0,
            chan: channel,
        }
    }

    #[inline]
    fn discard_buffer(self: Pin<&mut Self>) {
        let this = self.project();
        *this.pos = 0;
        *this.cap = 0;
    }

    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        let this = self.project();

        // If we've reached the end of our internal buffer then we need to fetch
        // some more data from the underlying reader.
        // Branch using `>=` instead of the more correct `==`
        // to tell the compiler that the pos..cap slice is always valid.
        if *this.pos >= *this.cap {
            debug_assert!(*this.pos == *this.cap);
            *this.cap = ready!(this.chan.poll_read(cx, this.buf))?;
            *this.pos = 0;
        }
        Poll::Ready(Ok(&this.buf[*this.pos..*this.cap]))
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        *self.project().pos = cmp::min(self.pos + amt, self.cap);
    }
}

impl<C> AsyncRead for TBufferedReadTransport<C>
where
    C: AsyncRead,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if self.pos == self.cap && buf.len() >= self.buf.len() {
            let res = self.as_mut().project().chan.poll_read(cx, buf);
            self.discard_buffer();
            return Poll::Ready(ready!(res));
        }

        let mut remaining = ready!(self.as_mut().poll_fill_buf(cx))?;
        let nread = remaining.read(buf)?;
        self.consume(nread);
        Poll::Ready(Ok(nread))
    }
}

/// Factory for creating instances of `TBufferedReadTransport`.
#[derive(Default)]
pub struct TBufferedReadTransportFactory;

impl TBufferedReadTransportFactory {
    pub fn new() -> TBufferedReadTransportFactory {
        TBufferedReadTransportFactory {}
    }
}

impl TReadTransportFactory for TBufferedReadTransportFactory {
    /// Create a `TBufferedReadTransport`.
    fn create(
        &self,
        channel: Box<dyn AsyncRead + Send + Unpin>,
    ) -> Box<dyn TReadTransport + Send + Unpin> {
        Box::new(TBufferedReadTransport::new(channel))
    }
}

#[pin_project]
#[derive(Debug)]
pub struct TBufferedWriteTransport<C>
where
    C: AsyncWrite,
{
    buf: Vec<u8>,
    cap: usize,
    written: usize,
    #[pin]
    chan: C,
}

impl<C> TBufferedWriteTransport<C>
where
    C: AsyncWrite,
{
    /// Create a `TBufferedTransport` with default-sized internal read and
    /// write buffers that wraps the given `TIoChannel`.
    pub fn new(channel: C) -> TBufferedWriteTransport<C> {
        TBufferedWriteTransport::with_capacity(WRITE_CAPACITY, channel)
    }

    /// Create a `TBufferedTransport` with an internal read buffer of size
    /// `read_capacity` and an internal write buffer of size
    /// `write_capacity` that wraps the given `TIoChannel`.
    pub fn with_capacity(write_capacity: usize, channel: C) -> TBufferedWriteTransport<C> {
        assert!(
            write_capacity > 0,
            "write buffer size must be a positive integer"
        );

        TBufferedWriteTransport {
            buf: Vec::with_capacity(write_capacity),
            cap: write_capacity,
            chan: channel,
            written: 0,
        }
    }

    fn flush_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut this = self.project();

        let len = this.buf.len();
        let mut ret = Ok(());
        while *this.written < len {
            match ready!(this
                .chan
                .as_mut()
                .poll_write(cx, &this.buf[*this.written..]))
            {
                Ok(0) => {
                    ret = Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write the buffered data",
                    ));
                    break;
                }
                Ok(n) => *this.written += n,
                Err(e) => {
                    ret = Err(e);
                    break;
                }
            }
        }
        if *this.written > 0 {
            this.buf.drain(..*this.written);
        }
        *this.written = 0;
        Poll::Ready(ret)
    }
}

impl<C> AsyncWrite for TBufferedWriteTransport<C>
where
    C: AsyncWrite,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.buf.len() + buf.len() > self.buf.capacity() {
            ready!(self.as_mut().flush_buf(cx))?;
        }
        if buf.len() >= self.buf.capacity() {
            self.project().chan.poll_write(cx, buf)
        } else {
            Poll::Ready(self.project().buf.write(buf))
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        ready!(self.as_mut().flush_buf(cx))?;
        self.project().chan.poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        ready!(self.as_mut().flush_buf(cx))?;
        self.project().chan.poll_close(cx)
    }
}

/// Factory for creating instances of `TBufferedWriteTransport`.
#[derive(Default)]
pub struct TBufferedWriteTransportFactory;

impl TBufferedWriteTransportFactory {
    pub fn new() -> TBufferedWriteTransportFactory {
        TBufferedWriteTransportFactory {}
    }
}

impl TWriteTransportFactory for TBufferedWriteTransportFactory {
    /// Create a `TBufferedWriteTransport`.
    fn create(
        &self,
        channel: Box<dyn AsyncWrite + Send + Unpin>,
    ) -> Box<dyn TWriteTransport + Send + Unpin> {
        Box::new(TBufferedWriteTransport::new(channel))
    }
}
