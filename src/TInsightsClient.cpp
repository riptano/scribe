/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <limits>
#include <cstdlib>
#include <sstream>
#include <boost/algorithm/string.hpp>
#include <zlib.h>

#include "TInsightsClient.h"
#include <thrift/transport/TSocket.h>

namespace apache {
namespace thrift {
namespace transport {

using namespace std;

static const int DEFAULT_BUFFER_SIZE = 512u;
static const int WINDOW_BITS = 15;
static const int GZIP_ENCODING = 16;

TInsightsClient::TInsightsClient(boost::shared_ptr<TTransport> transport,
                         std::string host, std::string path,
                         std::string bearerToken)
  : THttpTransport(transport),
    host_(host),
    path_(path),
    bearerToken(bearerToken),
    tBufSize_(0),
    httpStatusCode(0),
    tBuf_(NULL) {
}

TInsightsClient::~TInsightsClient() {
}

uint32_t TInsightsClient::readAll() {
  return this->readMoreData();
}

int TInsightsClient::getHttpStatusCode() {
    return httpStatusCode;
}

void TInsightsClient::parseHeader(char* header) {
  char* colon = strchr(header, ':');
  if (colon == NULL) {
    return;
  }
  char* value = colon + 1;

  if (boost::istarts_with(header, "Transfer-Encoding")) {
    if (boost::iends_with(value, "chunked")) {
      chunked_ = true;
    }
  } else if (boost::istarts_with(header, "Content-Length")) {
    chunked_ = false;
    contentLength_ = atoi(value);
  }
}

bool TInsightsClient::parseStatusLine(char* status) {
  char* http = status;

  char* code = strchr(http, ' ');
  if (code == NULL) {
    throw TTransportException(string("Bad Status: ") + status);
  }

  *code = '\0';
  while (*(code++) == ' ') {
  };

  char* msg = strchr(code, ' ');
  if (msg == NULL) {
    throw TTransportException(string("Bad Status: ") + status);
  }
  *msg = '\0';

  httpStatusCode = atoi(code);

  if (strcmp(code, "200") == 0) {
    // HTTP 200 = OK, we got the response
    return true;
  } else if (strcmp(code, "100") == 0) {
    // HTTP 100 = continue, just keep reading
    return false;
  } else {
    throw TTransportException(string("Bad Status: ") + status);
  }
}

void TInsightsClient::flush() {
  // Fetch the contents of the write buffer
  uint32_t len = deflateData();
  uint8_t* buf = tBuf_.get();

  // Construct the HTTP header
  std::ostringstream h;
  h << "POST " << path_ << " HTTP/1.1" << CRLF << "Host: " << host_ << CRLF
    << "Content-Type: application/vnd.insights.insightsStream+json" << CRLF << "Content-Length: " << len << CRLF
    << "Content-Encoding: gzip" << CRLF
    << "Accept: application/x-thrift" << CRLF << "User-Agent: Insights/" << VERSION << CRLF
    << "Insights-Namespace: dse.insights" << CRLF 
    << "Authorization: Bearer " << bearerToken << CRLF << CRLF;
  string header = h.str();

  if (header.size() > (std::numeric_limits<uint32_t>::max)())
    throw TTransportException("Header too big");
  // Write the header, then the data, then flush
  transport_->write((const uint8_t*)header.c_str(), static_cast<uint32_t>(header.size()));
  transport_->write(buf, len);
  transport_->flush();

  // Reset the buffer and header variables
  writeBuffer_.resetBuffer();
  readHeaders_ = true;
  httpStatusCode = 0;
}

/**
 * The buffer should be slightly larger than write buffer size due to
 * compression transforms (that may slightly grow on small frame sizes)
 */
void TInsightsClient::resizeTransformBuffer(uint32_t wBufSize, uint32_t additionalSize) {
  if (tBufSize_ < wBufSize + DEFAULT_BUFFER_SIZE) {
    uint32_t new_size = wBufSize + DEFAULT_BUFFER_SIZE + additionalSize;
    uint8_t* new_buf = new uint8_t[new_size];
    tBuf_.reset(new_buf);
    tBufSize_ = new_size;
  }
}

/*void TInsightsClient::inflateData() {
      z_stream stream;
      int err;

      stream.next_in = ptr;
      stream.avail_in = sz;

      // Setting these to 0 means use the default free/alloc functions
      stream.zalloc = (alloc_func)0;
      stream.zfree = (free_func)0;
      stream.opaque = (voidpf)0;
      err = inflateInit(&stream);
      if (err != Z_OK) {
        throw TApplicationException(TApplicationException::MISSING_RESULT,
                                    "Error while zlib deflateInit");
      }
      stream.next_out = tBuf_.get();
      stream.avail_out = tBufSize_;
      err = inflate(&stream, Z_FINISH);
      if (err != Z_STREAM_END || stream.avail_out == 0) {
        throw TApplicationException(TApplicationException::MISSING_RESULT,
                                    "Error while zlib deflate");
      }
      sz = stream.total_out;

      err = inflateEnd(&stream);
      if (err != Z_OK) {
        throw TApplicationException(TApplicationException::MISSING_RESULT,
                                    "Error while zlib deflateEnd");
      }

      memcpy(ptr, tBuf_.get(), sz);
}*/

uint32_t TInsightsClient::deflateData() {
      uint8_t* ptr;
      uint32_t sz;
      writeBuffer_.getBuffer(&ptr, &sz);

      z_stream stream;
      int err;

      stream.next_in = ptr;
      stream.avail_in = sz;

      stream.zalloc = (alloc_func)0;
      stream.zfree = (free_func)0;
      stream.opaque = (voidpf)0;

      err = deflateInit2 (&stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
                          WINDOW_BITS | GZIP_ENCODING,
                          8,
                          Z_DEFAULT_STRATEGY);

      if (err != Z_OK) {
        throw TTransportException(TTransportException::CORRUPTED_DATA,
                                  "Error while zlib deflateInit");
      }

      uint32_t tbuf_size = 0;
      while (err == Z_OK) {
        resizeTransformBuffer(sz, tbuf_size);

        stream.next_out = tBuf_.get();
        stream.avail_out = tBufSize_;
        err = deflate(&stream, Z_FINISH);
        tbuf_size += DEFAULT_BUFFER_SIZE;
      }
      sz = stream.total_out;

      err = deflateEnd(&stream);
      if (err != Z_OK) {
        throw TTransportException(TTransportException::CORRUPTED_DATA,
                                  "Error while zlib deflateEnd");
      }

      return sz;
}
}
}
} // apache::thrift::transport
