/*
 * This file is part of PowerDNS or dnsdist.
 * Copyright -- PowerDNS.COM B.V. and its contributors
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of version 2 of the GNU General Public License as
 * published by the Free Software Foundation.
 *
 * In addition, for the avoidance of any doubt, permission is granted to
 * link this program with OpenSSL and to (re)distribute the binaries
 * produced as the result of such linking.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */
#define CATCH_CONFIG_MAIN
#include <memory>
#include <catch2/catch_config.hpp>
#include "dnsdist.hh"
#include "dnsdist-lua.hh"
#include "dnsdist-rings.hh"
#include "dnsdist-xsk.hh"
#include "dnsdist-tcp.hh"
#include "dnsdist-udp.hh"

// NOTE: This file contains waaaaaay too many mocked things to make bench-dnsdist-action-rcode.cc
// link. In the future, all these functions and declarations should go away and be put into their
// own hh/cc files.

shared_ptr<BPFFilter> g_defaultBPFFilter{nullptr};
Rings g_rings;
string g_outputBuffer;

std::shared_ptr<dnsdist::udp::UDPTCPCrossQuerySender> dnsdist::udp::UDPCrossProtocolQuery::s_sender = std::make_shared<UDPTCPCrossQuerySender>();

void doExitNicely(int exitCode);
void doExitNicely([[maybe_unused]] int exitCode) {
};

void handleServerStateChange([[maybe_unused]] const string& nameWithAddr, [[maybe_unused]] bool newResult)
{
}
