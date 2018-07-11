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

#ifndef _THRIFT_TRANSPORT_TINSIGHTCLIENT_H_
#define _THRIFT_TRANSPORT_TINSIGHTCLIENT_H_ 1

#include <thrift/transport/THttpTransport.h>

namespace apache {
namespace thrift {
namespace transport {

class TInsightsClient : public THttpTransport {
public:
  TInsightsClient(boost::shared_ptr<TTransport> transport, std::string host, std::string path, std::string bearerToken);

  virtual ~TInsightsClient();

  virtual void flush();

  int getHttpStatusCode();
  uint32_t readAll();

protected:
  std::string host_;
  std::string path_;
  std::string bearerToken;

  int httpStatusCode;

  // Buffers to use for transform processing
  uint32_t tBufSize_;
  boost::scoped_array<uint8_t> tBuf_;

//  void inflateData();
  uint32_t deflateData();

  virtual void parseHeader(char* header);
  virtual bool parseStatusLine(char* status);

   void resizeTransformBuffer(uint32_t wBufSize, uint32_t additionalSize = 0);

};
}
}
} // apache::thrift::transport

#endif // #ifndef _THRIFT_TRANSPORT_TINSIGHTCLIENT_H_
