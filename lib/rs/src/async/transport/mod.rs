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

//! Types used to send and receive bytes over an I/O channel in an async way.
//!
//! The core types are the `TReadTransport`, `TWriteTransport` and the
//! `TIoChannel` traits, through which `TInputProtocol` or
//! `TOutputProtocol` can receive and send primitives over the wire. While
//! `TInputProtocol` and `TOutputProtocol` instances deal with language primitives
//! the types in this module understand only bytes.

mod buffered;

use std::{
    io,
    ops::{Deref, DerefMut},
    pin::Pin,
    task::{self, Context},
};

use futures::{AsyncRead, AsyncWrite};
use task::Poll;

/// Identifies a transport used by a `TInputProtocol` to receive bytes.
pub trait TReadTransport: AsyncRead {}

/// Helper type used by a server to create `TReadTransport` instances for
/// accepted client connections.
pub trait TReadTransportFactory {
    /// Create a `TTransport` that wraps a channel over which bytes are to be read.
    fn create(
        &self,
        channel: Box<dyn AsyncRead + Send + Unpin>,
    ) -> Box<dyn TReadTransport + Send + Unpin>;
}

/// Identifies a transport used by `TOutputProtocol` to send bytes.
pub trait TWriteTransport: AsyncWrite {}

/// Helper type used by a server to create `TWriteTransport` instances for
/// accepted client connections.
pub trait TWriteTransportFactory {
    /// Create a `TTransport` that wraps a channel over which bytes are to be sent.
    fn create(
        &self,
        channel: Box<dyn AsyncWrite + Send + Unpin>,
    ) -> Box<dyn TWriteTransport + Send + Unpin>;
}

impl<T> TReadTransport for T where T: AsyncRead + Unpin {}

impl<T> TWriteTransport for T where T: AsyncWrite + Unpin {}

// FIXME: implement the Debug trait for boxed transports

impl<T> TReadTransportFactory for Box<T>
where
    T: TReadTransportFactory + ?Sized,
{
    fn create(
        &self,
        channel: Box<dyn AsyncRead + Send + Unpin>,
    ) -> Box<dyn TReadTransport + Send + Unpin> {
        (**self).create(channel)
    }
}

impl<T> TWriteTransportFactory for Box<T>
where
    T: TWriteTransportFactory + ?Sized,
{
    fn create(
        &self,
        channel: Box<dyn AsyncWrite + Send + Unpin>,
    ) -> Box<dyn TWriteTransport + Send + Unpin> {
        (**self).create(channel)
    }
}

/// Identifies a splittable bidirectional I/O channel used to send and receive bytes in a
/// non-blocking way.
pub trait TIoChannel: AsyncRead + AsyncWrite + Unpin {
    /// Split the channel into a readable half and a writable half, where the
    /// readable half implements `io::Read` and the writable half implements
    /// `io::Write`. Returns `None` if the channel was not initialized, or if it
    /// cannot be split safely.
    ///
    /// Returned halves may share the underlying OS channel or buffer resources.
    /// Implementations **should ensure** that these two halves can be safely
    /// used independently by concurrent threads.
    fn split(self) -> crate::Result<(ReadHalf<Self>, WriteHalf<Self>)>
    where
        Self: Sized;
}

/// The readable half of an object returned from `TIoChannel::split`.
#[derive(Debug)]
pub struct ReadHalf<C>
where
    C: AsyncRead + Unpin,
{
    handle: C,
}

/// The writable half of an object returned from `TIoChannel::split`.
#[derive(Debug)]
pub struct WriteHalf<C>
where
    C: AsyncWrite + Unpin,
{
    handle: C,
}

impl<C> ReadHalf<C>
where
    C: AsyncRead + Unpin,
{
    /// Create a `ReadHalf` associated with readable `handle`
    pub fn new(handle: C) -> ReadHalf<C> {
        ReadHalf { handle }
    }
}

impl<C> WriteHalf<C>
where
    C: AsyncWrite + Unpin,
{
    /// Create a `WriteHalf` associated with writable `handle`
    pub fn new(handle: C) -> WriteHalf<C> {
        WriteHalf { handle }
    }
}

impl<C> AsyncRead for ReadHalf<C>
where
    C: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.handle).poll_read(cx, buf)
    }
}

impl<C> AsyncWrite for WriteHalf<C>
where
    C: AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.handle).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.handle).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.handle).poll_close(cx)
    }
}

impl<C> Deref for ReadHalf<C>
where
    C: AsyncRead + Unpin,
{
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl<C> DerefMut for ReadHalf<C>
where
    C: AsyncRead + Unpin,
{
    fn deref_mut(&mut self) -> &mut C {
        &mut self.handle
    }
}

impl<C> Deref for WriteHalf<C>
where
    C: AsyncWrite + Unpin,
{
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl<C> DerefMut for WriteHalf<C>
where
    C: AsyncWrite + Unpin,
{
    fn deref_mut(&mut self) -> &mut C {
        &mut self.handle
    }
}
