/**
* Copyright 2021 Comcast Cable Communications Management, LLC
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
* SPDX-License-Identifier: Apache-2.0
*/

#include "rtRemoteConfig.h"
#include "rtRemoteEndPoint.h"
#include "rtRemoteEnvironment.h"
#include "rtRemoteMessage.h"
#include "rtRemoteSocketBuffer.h"
#include "rtRemoteSocketUtils.h"
#include "rtRemoteUnicastResolver.h"
#include "rtRemoteObjectCache.h"

#include <chrono>

#include <arpa/inet.h>
#include <rtLog.h>
#include <unistd.h>

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace
{
  std::string
  to_string(rtRemoteMessagePtr const& m)
  {
    rapidjson::StringBuffer buff;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buff);
    m->Accept(writer);
    return buff.GetSize() > 0
      ? std::string(buff.GetString(), buff.GetSize())
      : std::string();
  }
}

rtRemoteUnicastResolver::rtRemoteUnicastResolver(rtRemoteEnvironment* env)
  : m_pid(getpid())
  , m_message_reader(nullptr)
  , m_daemon_connection(-1)
  , m_env(env)
{
  m_buff.reserve(1024 * 1024);
  m_buff.resize(1024 * 1024);

  memset(&m_daemon_endpoint, 0, sizeof(m_daemon_endpoint));
  memset(&m_rpc_endpoint, 0, sizeof(m_rpc_endpoint));
}

rtRemoteUnicastResolver::~rtRemoteUnicastResolver()
{
  this->close();
}

rtError
rtRemoteUnicastResolver::reregisterObjects()
{
  // Look through our cache and re-register all locally known objects
  std::vector<std::string> objIds = m_env->ObjectCache->getAllIds();
  rtError err;

  for (auto &&obj : objIds)
  {
    err = registerObject(obj, m_rpc_endpoint);
    if (err != RT_OK)
    {
      rtLogWarn("Failed to re-register object %s with error %s", obj.c_str(), rtStrError(err));
    }
  }

  return RT_OK;
}

rtError
rtRemoteUnicastResolver::open(sockaddr_storage const& rpc_endpoint)
{
  m_rpc_endpoint = rpc_endpoint;
  rtError err = connectToResolverServer();
  if (err == RT_OK)
    m_message_reader = new std::thread(&rtRemoteUnicastResolver::readIncomingMessages, this);
  return err;
}

rtError
rtRemoteUnicastResolver::connectToResolverServer()
{
  rtError err = rtParseAddress(m_daemon_endpoint, m_env->Config->resolver_unicast_address().c_str(),
    m_env->Config->resolver_unicast_port(), nullptr);
  if (err != RT_OK)
  {
    rtLogError("failed to parse unicast resolver address/port %s/%u. %s",
      m_env->Config->resolver_unicast_address().c_str(),
      m_env->Config->resolver_unicast_port(),
      rtStrError(err));
    return err;
  }

  m_daemon_connection = socket(m_daemon_endpoint.ss_family, SOCK_STREAM, 0);
  if (m_daemon_connection < 0)
  {
    err = rtErrorFromErrno(errno);
    std::string s = rtSocketToString(m_daemon_endpoint);
    rtLogError("failed to create client tcp connection to unicast resolver %s. %s", s.c_str(),
      rtStrError(err));
    return err;
  }

  socklen_t len;
  rtSocketGetLength(m_daemon_endpoint, &len);
  int ret = connect(m_daemon_connection, reinterpret_cast<sockaddr *>(&m_daemon_endpoint), len);
  if (ret < 0)
  {
    err = rtErrorFromErrno(errno);
    std::string s = rtSocketToString(m_daemon_endpoint);
    rtLogError("failed to connect to unicast resolver %s. %s", s.c_str(), rtStrError(err));
    ::close(m_daemon_connection);
    m_daemon_connection = -1;
    return err;
  }

  return err;
}

void
rtRemoteUnicastResolver::readIncomingMessages()
{
  rtRemoteMessagePtr incoming_message;

  while (true)
  {
    incoming_message.reset();
    rtError err = rtReadMessage(m_daemon_connection, m_buff, incoming_message);
    if (err == RT_OK)
    {
      rtRemoteCorrelationKey seqId = rtMessage_GetCorrelationKey(*incoming_message);

      std::unique_lock<std::mutex> lock(m_mutex);
      auto itr = m_outstanding_requests.find(seqId);
      if (itr != m_outstanding_requests.end())
      {
        itr->second.Response = incoming_message;
      }
      else
      {
        // it's probably a keep_alive
        char const* type = rtMessage_GetMessageType(*incoming_message);
        rtLogInfo("msg:%s", type);
      }
      lock.unlock();
      m_cond.notify_all();
    }
    else
    {
      if (err == rtErrorFromErrno(ENOTCONN))
      {
        reconnect();
      }
      else
      {
        rtLogWarn("failed to read incoming message. %s", rtStrError(err));
      }
    }
  }
}

rtError
rtRemoteUnicastResolver::close()
{
  if (m_daemon_connection != -1)
  {
    rtLogInfo("closing daemon connection fd:%d", m_daemon_connection);
    ::close(m_daemon_connection);
    m_daemon_connection = -1;
  }

  return RT_OK;
}

rtError
rtRemoteUnicastResolver::sendRequest(rapidjson::Document const& req, rtRemoteMessagePtr& res, uint32_t timeout)
{
  rtError err = RT_OK;

  std::pair< RequestContextMap::iterator, bool > entry;

  {
    rtRemoteCorrelationKey seqId = rtRemoteCorrelationKey::fromString(req[kFieldNameCorrelationKey].GetString());

    RequestContext requestContext(seqId);
    requestContext.Response = nullptr;

    std::unique_lock<std::mutex> lock(m_mutex);
    entry = m_outstanding_requests.insert(std::make_pair(seqId, requestContext));
    if (!entry.second)
    {
      rtLogError("failed to insert into outstanding request map");
      return RT_FAIL;
    }

    err = rtSendDocument(req, m_daemon_connection, nullptr);
    if (err != RT_OK)
    {
      if (err == rtErrorFromErrno(EPIPE))
        m_daemon_connection = -1;
    }
  }

  if (err == RT_OK)
  {
    auto now = std::chrono::system_clock::now();
    std::unique_lock<std::mutex> lock(m_mutex);
    if (m_cond.wait_until(lock, now + std::chrono::milliseconds(timeout),
        [entry]{ return entry.first->second.Response != nullptr; }))
      res = entry.first->second.Response;
    else
    {
      m_outstanding_requests.erase(entry.first);
      err = RT_ERROR_TIMEOUT;
    }
  }

  return err;
}

rtError
rtRemoteUnicastResolver::locateObject(std::string const& name, sockaddr_storage& endpoint, uint32_t timeout)
{
  if (m_daemon_connection == -1)
    return RT_NO_CONNECTION;

  rapidjson::Document req;
  req.SetObject();
  req.AddMember(kFieldNameMessageType, kNsMessageTypeLookup, req.GetAllocator());
  req.AddMember(kFieldNameObjectId, name, req.GetAllocator());
  req.AddMember(kFieldNameSenderId, m_pid, req.GetAllocator());
  req.AddMember(kFieldNameTimeout, 4000, req.GetAllocator());

  rtRemoteCorrelationKey seqId = rtMessage_GetNextCorrelationKey();
  req.AddMember(kFieldNameCorrelationKey, seqId.toString(), req.GetAllocator());

  rtRemoteMessagePtr res;
  rtError err = sendRequest(req, res, timeout);
  if (err != RT_OK)
    return err;

  auto remote_endpoint = res->FindMember(kFieldNameEndPoint);
  if (remote_endpoint != res->MemberEnd())
  {
    std::unique_ptr<rtRemoteEndPoint> e(rtRemoteEndPoint::fromString(remote_endpoint->value.GetString()));
    endpoint = e->toSockAddr();
  }
  else
  {
    rtLogError("unicast resolver returned response without endpoint.");
    err = RT_ERROR_PROTOCOL_ERROR;
  }

  return err;
}

rtError
rtRemoteUnicastResolver::unregisterObject(std::string const& name)
{
  if (m_daemon_connection == -1)
    return RT_NO_CONNECTION;

  rapidjson::Document req;
  req.SetObject();
  req.AddMember(kFieldNameMessageType, kNsMessageTypeDeregister, req.GetAllocator());
  req.AddMember(kFieldNameObjectId, name, req.GetAllocator());
  req.AddMember(kFieldNameSenderId, m_pid, req.GetAllocator());

  rtRemoteCorrelationKey seqId = rtMessage_GetNextCorrelationKey();
  req.AddMember(kFieldNameCorrelationKey, seqId.toString(), req.GetAllocator());

  rtRemoteMessagePtr res;
  rtError err = sendRequest(req, res, 1000);
  if (err != RT_OK)
    return err;

  auto itr = res->FindMember(kNsFieldNameStatusCode);
  if (itr != res->MemberEnd())
    err = static_cast<rtError>(itr->value.GetInt());
  else
    err = RT_ERROR_PROTOCOL_ERROR;

  return err;
}

rtError
rtRemoteUnicastResolver::registerObject(std::string const& name, sockaddr_storage const& endpoint)
{
  if (m_daemon_connection == -1)
    return RT_NO_CONNECTION;

  rapidjson::Document req;
  req.SetObject();
  req.AddMember(kFieldNameMessageType, kNsMessageTypeRegister, req.GetAllocator());
  req.AddMember(kFieldNameObjectId, name, req.GetAllocator());
  req.AddMember(kFieldNameSenderId, m_pid, req.GetAllocator());

  rtRemoteCorrelationKey seqId = rtMessage_GetNextCorrelationKey();
  req.AddMember(kFieldNameCorrelationKey, seqId.toString(), req.GetAllocator());
  req.AddMember(kFieldNameEndPoint, rtSocketToString(endpoint), req.GetAllocator());

  rtRemoteMessagePtr res;
  rtError err = sendRequest(req, res, 1000);
  if (err != RT_OK)
    return err;

  auto itr = res->FindMember(kNsFieldNameStatusCode);
  if (itr != res->MemberEnd())
    err = static_cast<rtError>(itr->value.GetInt());
  else
    err = RT_ERROR_PROTOCOL_ERROR;

  return err;
}

rtError
rtRemoteUnicastResolver::reconnect()
{
  rtLogError("daemon connection broken, attempting to re-connect");
  useconds_t delay = 500000;            // 0.5s
  const useconds_t maxDelay = 60000000; // 60s
  int attempt = 0;

  rtError reconnectionErr;

  while ((reconnectionErr = connectToResolverServer()) != RT_OK)
  {
    attempt++;
    rtLogWarn("Connection to daemon failed - attempt %d", attempt);
    usleep(delay);
    if (delay < maxDelay)
    {
      delay *= 2;
    }
    else
    {
      break;
    }
  }

  if (reconnectionErr == RT_OK)
  {
    rtLogInfo("Reconnected!");
    m_reconnection = new std::thread(&rtRemoteUnicastResolver::reregisterObjects, this);
  }
}
