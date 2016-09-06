################################################################################
#          _____ _
#         |_   _| |_  ___
#           | | | ' \/ -_)
#           |_| |_||_\___|
#                   _   _             ____            _           _
#    / \   _ __ ___| |_(_) ___ __ _  |  _ \ _ __ ___ (_) ___  ___| |_
#   / _ \ | '__/ __| __| |/ __/ _` | | |_) | '__/ _ \| |/ _ \/ __| __|
#  / ___ \| | | (__| |_| | (_| (_| | |  __/| | | (_) | |  __/ (__| |_
# /_/   \_\_|  \___|\__|_|\___\__,_| |_|   |_|  \___// |\___|\___|\__|
#                                                  |__/
#          The Arctica Modular Remote Computing Framework
#
################################################################################
#
# Copyright (C) 2015-2016 The Arctica Project 
# http://http://arctica-project.org/
#
# This code is dual licensed: strictly GPL-2 or AGPL-3+
#
# GPL-2
# -----
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the
# Free Software Foundation, Inc.,
#
# 51 Franklin St, Fifth Floor, Boston, MA 02110-1301, USA.
#
# AGPL-3+
# -------
# This programm is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This programm is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program; if not, write to the
# Free Software Foundation, Inc.,
# 51 Franklin St, Fifth Floor, Boston, MA 02110-1301, USA.
#
# Copyright (C) 2015-2016 Guangzhou Nianguan Electronics Technology Co.Ltd.
#                         <opensource@gznianguan.com>
# Copyright (C) 2015-2016 Mike Gabriel <mike.gabriel@das-netzwerkteam.de>
#
################################################################################
package Arctica::Core::JABus::Socket::RandomPortTCP;
use strict;
use Exporter qw(import);
use IO::Socket::INET;
# Be very selective about what (if any) gets exported by default:
our @EXPORT = qw( getRandomPortTCP );
# And be mindfull of what we lett the caller request here too:
our @EXPORT_OK = qw( );

sub getRandomPortTCP {
	# Dirty little hack that gets you a random availalbe TCP port
	# Its advisable to catch bind failure and request a new port 
	# in the unlikely event bind to port fails.
	# Makes prediction based DoS harder and avoids a few other issues...
	# Need to load these values from a config file in /etc/arctica/
	my $portrange_min = 30000;
	my $portrange_max = 50000;
	my $attempt_max = 1000;
	my $attempt_cnt = 0;
	while ($attempt_cnt <= $attempt_max) {
		my $random_port = int($portrange_min + rand($portrange_max - $portrange_min));
		unless (IO::Socket::INET->new(	
			PeerAddr => 'localhost',
			PeerPort => $random_port,
			Proto    => 'tcp')) {
			return $random_port;
		}
		$attempt_cnt++;
	}
	return 0;
}


