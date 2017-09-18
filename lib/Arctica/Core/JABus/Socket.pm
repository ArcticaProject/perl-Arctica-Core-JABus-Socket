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
package Arctica::Core::JABus::Socket;
use strict;
use Exporter qw(import);
#use Glib 'TRUE', 'FALSE';
use Data::Dumper;
use JSON;# Should we use the OO style apporach?
use Arctica::Core::eventInit qw( genARandom BugOUT );
use IO::Socket::UNIX qw( SOCK_STREAM SOMAXCONN );
use Arctica::Core::ManageDirs qw( permZealot );
# Be very selective about what (if any) gets exported by default:
our @EXPORT = qw(genARandom);
# And be mindfull of what we lett the caller request here too:
our @EXPORT_OK = qw( BugOUT bugOutCfg );

my $arctica_core_object;

# * NOTE * NOTE * NOTE * NOTE * NOTE * NOTE * NOTE * NOTE * NOTE * NOTE * NOTE *
# PS! DON'T FORGET STRICT FILE PREMS FOR SOCKETS ETC....
# * NOTE * NOTE * NOTE * NOTE * NOTE * NOTE * NOTE * NOTE * NOTE * NOTE * NOTE *
# DIE on obvious developer mistakes.... WARN on stuff that could potentially 
# happen dynamically during runtime.

################################################################################
# Are we even using this at all....?!?! 
# Maybe later? For now we're just leeping it here as a reminder...
#
# EVERY CONF VALUE THAT IS READ ONLY (RO) only need only have two pieces 
# Of information  "ro => 1," and "value => 'what ever'".
# Any value that may be set by the application developer need to have a 
# sanitation routine attached to it, be it one a standart Arctica one,
# or a custom private one. It the module/lib in question is the only use 
# case for a particular rule. Duplicate the standard sanitation function and 
# clear out the standard rules and attach a "priv_saneconf" at the bottom of 
# the code section of this file. If your "nonstandard" sanitation rule fits on 
# a line or so... you may use an anonymus sub. 
# the 'critical' value determin if a sanitation failure should result in death
# or simple failure to set the unsanitized value.
my %_conf_template = (
	dval1 => {
		ro => 0,
		sanitation => sub {return $_[0];},
		critical => 0,
		value => 'something default'
	},
);
#
################################################################################

sub new {
	BugOUT(9,"JABus Socket new->ENTER");
	my $class_name = $_[0];# Be EXPLICIT!! DON'T SHIFT OR "@_";
	$arctica_core_object = $_[1];
	my $the_tmpdir = $arctica_core_object->{'a_dirs'}{'tmp_adir'};
	my $dev_conf = $_[2];
	my $socket_destination = lc($dev_conf->{'destination'});
	my $S_or_C;
	my $socket_type = lc($dev_conf->{'type'});
	if ($dev_conf->{'is_server'} eq 1)  {
		$S_or_C = "Server";
	} elsif ($dev_conf->{'is_client'} eq 1) {
		$S_or_C = "Client";
	} elsif (($dev_conf->{'is_server'} eq 1) and ($dev_conf->{'is_client'} eq 1)) {
		die("same socket object can't be both server and client!");
	} else {
		die("socket 'is_client' or 'is_server' must be set!");
	}
	unless (($socket_destination eq 'local') or ($socket_destination eq 'remote')) {
		die("socket destination can only be 'local' or 'remote'!");
	}
	BugOUT(2,"Creating new socket object");
	my %conf = %_conf_template;
	my $self = {
		type => $socket_type,
		destination => $socket_destination,
		conf => \%conf,# Change THIS LATER TO Make COPY of %conf?
		isArctica => 1, # Declare that this is a Arctica "something"
		aobject_name => "JABus_Socket_$S_or_C",
	};
	if ($dev_conf->{'hooks'}) {
		$self->{'hooks'} = $dev_conf->{'hooks'};
#		print "HOOKS:\t\n",Dumper($self->{'hooks'}),"\n";
	}
	$self->{'declare_id'} = $arctica_core_object->{'AExecDeclaration'};
	if ($dev_conf->{'handle_in_solo'} and $dev_conf->{'handle_in_dispatc'}) {
		die("JABus::Server: solo and dispatch at the same time? Not ok!");
	} elsif ($dev_conf->{'handle_in_solo'}) {
		$self->{'handle_in_solo'} = $dev_conf->{'handle_in_solo'};
	} elsif ($dev_conf->{'handle_in_dispatch'}) { 
		$self->{'handle_in_dispatch'} = $dev_conf->{'handle_in_dispatch'};
	}
	
	unless ($self->{'handle_in_dispatch'} or $self->{'handle_in_solo'}) {
		die("You need to define something for handle_in_dispatch or handle_in_solo");
	}
	
	if ($S_or_C eq "Server") {
		$self->{'_socket_id'} = genARandom('id');
		$self->{'_socket_auth_key'} = genARandom('key');
		BugOUT(1,"Server SocketID:	$self->{'_socket_id'}");
	} elsif ($S_or_C eq "Client") {
		if ($dev_conf->{'connect_to'}) {# ADD SANITATION HERE!!!!
			$self->{'_socket_id'} = $dev_conf->{'connect_to'};
			BugOUT(1,"Client SocketID:	$self->{'_socket_id'}");
		}
	}

	bless($self, $class_name);
	if ($S_or_C eq "Server") {

		BugOUT(1,"Initiating Server...");
		if ($socket_type eq "tcp")  {
			my $tmp_soc_dir;
			if ($socket_destination eq "local") {
				$tmp_soc_dir = "$the_tmpdir/soc/local";
			} elsif ($socket_destination eq "remote") {
				$tmp_soc_dir = "$the_tmpdir/soc/remote/out";
			} else {
				die("There must be a hole in the fabric of time and space!");
			}
			unless (-d $tmp_soc_dir) {
				die("JABus::Socket expects tmp dirs to be present.");
			}
			$self->{'s_or_c'} = "Server";
			if ($dev_conf->{'tcp_port'} =~ /(\d{1,5})/) {
				$self->{'tcp_port'} = $1;
			} else {
				die("Don't know what TCP port to use!");
			}
# FIXME	Add support for autoselection of ports here.... and using ports from predefined.
			$self->{'the_socket_path'} = "$tmp_soc_dir/$self->{'_socket_id'}";
			open(SOCINF,">$self->{'the_socket_path'}.inf");
			permZealot("$self->{'the_socket_path'}.inf");
			print SOCINF "tcp\n$self->{'tcp_port'}\n$self->{'_socket_auth_key'}\n";
			close(SOCINF);

			$self->{'the_socket'} = IO::Socket::INET->new(
					LocalAddr => "127.0.0.1",
					Proto => "tcp",
					LocalPort => $self->{'tcp_port'},
#					Local => $self->{'the_socket_path'},# do something "smart" here!
					Type => SOCK_STREAM,
					Reuse => 1,
					Timeout => 5,
					Listen => SOMAXCONN
				);
			if (defined $self->{'the_socket'}) {
				# Hand it over to a watcher...
				$self->{'the_fileno'} = fileno($self->{'the_socket'});
				$self->{'watcher'} = Glib::IO->add_watch(
					$self->{'the_fileno'}, 'in',
					sub {\$self->_server_accept_conn();}, 
					$self->{'the_socket'});
					BugOUT(1,"Initiation complete!");
			} else {
				die("Unable to initiate socket: $self->{'_socket_id'} ($tmp_soc_dir)");# DO SOMETHING ELSE HERE!
			}
		} elsif ($socket_type eq "unix") {
			my $tmp_soc_dir;
			if ($socket_destination eq "local") {
				$tmp_soc_dir = "$the_tmpdir/soc/local";
			} elsif ($socket_destination eq "remote") {
				$tmp_soc_dir = "$the_tmpdir/soc/remote/out";
			} else {
				die("There must be a hole in the fabric of time and space!");
			}
			unless (-d $tmp_soc_dir) {
				die("JABus::Socket expects tmp dirs to be present.");
			}
		
			$self->{'s_or_c'} = "Server";
			$self->{'the_socket_path'} = "$tmp_soc_dir/$self->{'_socket_id'}";
			open(SOCINF,">$self->{'the_socket_path'}.inf");
			permZealot("$self->{'the_socket_path'}.inf");
			print SOCINF "unix\n$self->{'the_socket_path'}.sock\n$self->{'_socket_auth_key'}\n";
			close(SOCINF);
			$self->{'the_socket'} = IO::Socket::UNIX->new(
					Local => "$self->{'the_socket_path'}.sock",# do something "smart" here!
					Type => SOCK_STREAM,
					Reuse => 1,
					Timeout => 5,
					Listen => SOMAXCONN
				);
			permZealot("$self->{'the_socket_path'}.sock");
			if (defined $self->{'the_socket'}) {
				# Hand it over to a watcher...
				$self->{'the_fileno'} = fileno($self->{'the_socket'});
				$self->{'watcher'} = Glib::IO->add_watch(
					$self->{'the_fileno'}, 'in',
					sub {\$self->_server_accept_conn();}, 
					$self->{'the_socket'});
					BugOUT(1,"Initiation complete!");
			} else {
				die("Unable to initiate socket: $self->{'_socket_id'} ($tmp_soc_dir)");# DO SOMETHING ELSE HERE!
			}
		}

	} elsif ($S_or_C eq "Client") {
		BugOUT(1,"Initiating Client..");

		if ($socket_type eq "tcp") {

			my $tmp_soc_dir;
			if ($socket_destination eq "local") {
				$tmp_soc_dir = "$the_tmpdir/soc/local";
			} elsif ($socket_destination eq "remote") {
				$tmp_soc_dir = "$the_tmpdir/soc/remote/in";
			} else {
				die("There must be a hole in the fabric of time and space!");
			}
			unless (-d $tmp_soc_dir) {
				die("JABus::Socket expects tmp dirs to be present. ($tmp_soc_dir)");
			}
			
			$self->{'s_or_c'} = "Client";
			$self->{'tcp_port'} = 5462;
			$self->{'the_socket_path'} = "$tmp_soc_dir/$self->{'_socket_id'}";
			BugOUT(9,"SOCKPATH: $self->{'the_socket_path'}.inf");
			open(SOCINF,"$self->{'the_socket_path'}.inf");
			my ($sinf_type,$sinf_port,$sinf_authkey) = <SOCINF>;
			close(SOCINF);
			if ($sinf_type =~ /tcp/) {
				$sinf_port =~ s/\D//g;
				if ($sinf_port =~ /(\d{1,5})/) { 
					$self->{'tcp_port'} = $1;
				} else {
					die("Eh...TCP Port# '$sinf_port' ?");
				}
				$sinf_authkey =~ s/[^a-zA-Z0-9]//g;
				if ($sinf_authkey =~ /([a-zA-Z0-9]{30,})/) {
					$self->{'_socket_auth_key'} = $1;
				} else {
					$self->{'_socket_auth_key'} = 0;
					warn("Invalid auth key...  Connection will probably fail...");
				}
			} else {
				die("Expecting 'tcp' but got '$sinf_type'");
			}

			$self->{'the_socket'} =  IO::Socket::INET->new(
					PeerAddr => "127.0.0.1",
					PeerPort => $self->{'tcp_port'}, 
					Type	=> SOCK_STREAM,
					Timeout	=> 5,
				);
			if (defined $self->{'the_socket'}) {
				$self->{'the_socket'}->autoflush(1);
				# Hand it over to a watcher...
				$self->{'the_fileno'} = fileno($self->{'the_socket'});# do we ever really use this?
					$self->{'watcher'} = Glib::IO->add_watch ( 
						$self->{'the_fileno'}, 
						['in', 'hup', 'err'], 
						sub {\$self->_client_handle_conn($_[1])},
						$self->{'the_socket'});

				# REMOVE THE FOLLOWING LINE... INSERT AUTH STUFFS HERE
				if ($self->{'handle_in_solo'}) {
					$self->_client_send({
						'JAB' => 'auth',
						'declare_id' => $self->{'declare_id'},
						'handler_type' => 'solo',
						'auth_key' => $self->{'_socket_auth_key'},
					});
				} elsif ($self->{'handle_in_dispatch'}) {
					my %to_send;
					foreach my $hidp_key (keys %{$self->{'handle_in_dispatch'}}) {#do some filtering here?!
#						print "\t\tKEY:\t$hidp_key\n";
						$to_send{$hidp_key} = 1;
					}
					$self->_client_send({
						'JAB' => 'auth',
						'declare_id' => $self->{'declare_id'},
						'handler_type' => 'dispatch',
						'dispatch_list' => \%to_send,
						'auth_key' => $self->{'_socket_auth_key'},
					});
				} else {
					die("THIS SHOULD NEVER HAPPEN");
				}


				BugOUT(1,"Initiation complete!");
			} else {
				die("Unable to initiate socket: $self->{'_socket_id'} ($tmp_soc_dir)");# DO SOMETHING ELSE HERE!
			}

		} elsif ($socket_type eq "unix") {
			my $tmp_soc_dir;
			if ($socket_destination eq "local") {
				$tmp_soc_dir = "$the_tmpdir/soc/local";
			} elsif ($socket_destination eq "remote") {
				$tmp_soc_dir = "$the_tmpdir/soc/remote/in";
			} else {
				die("There must be a hole in the fabric of time and space!");
			}
			unless (-d $tmp_soc_dir) {
				die("JABus::Socket expects tmp dirs to be present. ($tmp_soc_dir)");
			}
			
			$self->{'s_or_c'} = "Client";
			$self->{'the_socket_path'} = "$tmp_soc_dir/$self->{'_socket_id'}";
			$self->{'the_socket'} =  IO::Socket::UNIX->new(
				Peer	=> "$self->{'the_socket_path'}.sock",
				Type	=> SOCK_STREAM,
				Timeout	=> 5 );
			if (defined $self->{'the_socket'}) {
				$self->{'the_socket'}->autoflush(1);
				# Hand it over to a watcher...
				$self->{'the_fileno'} = fileno($self->{'the_socket'});# do we ever really use this?
					$self->{'watcher'} = Glib::IO->add_watch ( 
						$self->{'the_fileno'}, 
						['in', 'hup', 'err'], 
						sub {\$self->_client_handle_conn($_[1])},
						$self->{'the_socket'});

				# REMOVE THE FOLLOWING LINE... INSERT AUTH STUFFS HERE
				if ($self->{'handle_in_solo'}) {
					$self->_client_send({
						'JAB' => 'auth',
						'declare_id' => $self->{'declare_id'},
						'handler_type' => 'solo',
						'auth_key' => 0,
					});
				} elsif ($self->{'handle_in_dispatch'}) {
					my %to_send;
					foreach my $hidp_key (keys %{$self->{'handle_in_dispatch'}}) {#do some filtering here?!
#						print "\t\tKEY:\t$hidp_key\n";
						$to_send{$hidp_key} = 1;
					}
					$self->_client_send({
						'JAB' => 'auth',
						'declare_id' => $self->{'declare_id'},
						'handler_type' => 'dispatch',
						'dispatch_list' => \%to_send,
						'auth_key' => 0,
					});
				} else {
					die("THIS SHOULD NEVER HAPPEN");
				}


				BugOUT(1,"Initiation complete!");
			} else {
				die("Unable to initiate socket: $self->{'_socket_id'} ($tmp_soc_dir)");# DO SOMETHING ELSE HERE!
			}
		}
	}

	$arctica_core_object->{'aobj'}{'JABus'}{$S_or_C}{$self->{'_socket_id'}}
		= \$self;

	BugOUT(9,"JABus Socket new->DONE");

	return $self;
}

sub server_get_socket_id {
	my $self = $_[0];
	if ($self->{'_socket_id'}) {
		return $self->{'_socket_id'};
	} else {
		die("Another rupture in space&time!");
	}
}

sub server_send {
	BugOUT(9,"JABus::Socket server_send()->ENTER");
	my $self = $_[0];
	my $client_id = $_[1];
	if ($self->{'clients'}{$client_id}{'status'}{'auth'} eq 1) {
		if ($self->{'clients'}{$client_id}{'handler_type'} eq 'solo') {
			my $data_to_send = $_[2];
				$self->_server_send($client_id,{
					'data' => $data_to_send,
					'JAB' => 'data'
				});
		} elsif ($self->{'clients'}{$client_id}{'handler_type'} eq 'dispatch') {
			my $dispatch_to = lc($_[2]);
#			my $dispatch_data = $_[3];
			if ($dispatch_to =~ /^([a-z0-9\_]{2,32})$/) {
				$dispatch_to = $1;
				if ($self->{'clients'}{$client_id}{'dispatch_list'}{$dispatch_to} eq 1) {
					$self->_server_send($client_id,{
							'disp_to' => $dispatch_to,
							'data' => $_[3],
							'JAB' => 'data'
						});
				} else {
					die("JABus::Socket server_send() '$dispatch_to' is invalid dispatch alias");
				}
			} else {
				die("JABus::Socket server_send() DISPATCH TO WHERE?!!?!");
			}
		} else {
			die("THIS SHOULD NEVER HAPPEN!?!");
		}
	}
	BugOUT(9,"JABus::Socket server_send()->DONE");
}

sub _server_send {
	BugOUT(9,"JABus::Socket _server_send()->ENTER");
	my $self = $_[0];
	my $client_id = $_[1];

	my $json_data;
	eval {
		$json_data = encode_json($_[2]);
	} or warn("JABus::Socket _server_send() encode_json crapped it self!");

	if ($self->{'clients'}{$client_id}{'io_obj'} and $json_data) {
		my $bytes_written 
			= syswrite($self->{'clients'}{$client_id}{'io_obj'},
			"$json_data\n")
			 or warn("Error while trying to send to client ($!)");

		$self->{'clients'}{$client_id}{'bytes_written'}+=$bytes_written;
		$self->{'total_bytes_written'} += $bytes_written;
		BugOUT(8,"JABus::Socket Server Send->JAB Sent...");
	}

	BugOUT(9,"JABus::Socket _server_send()->DONE");
}

sub _server_accept_conn {
	BugOUT(9,"JABus::Socket Server _accept_conn()->ENTER");
	my $self = $_[0];

	my $new_client_id = genARandom('id');
	$self->{'clients'}{$new_client_id}{'io_obj'}
		= $self->{'the_socket'}->accept()
			or BugOUT(1,"Can't accept connection @_");
	$self->{'clients'}{$new_client_id}{'auth'} = 0;
	$self->{'clients'}{$new_client_id}{'io_obj'}->autoflush(1);
	$self->{'clients'}{$new_client_id}{'watcher'} = Glib::IO->add_watch ( 
		fileno( $self->{'clients'}{$new_client_id}{'io_obj'} ), 
		['in', 'hup', 'err'], 
		sub {\$self->_server_handle_conn($new_client_id,$_[1])},
		$self->{'clients'}{$new_client_id}{'io_obj'} );
		
	BugOUT(9,"JABus::Socket Server _accept_conn()->DONE");
	return 1;
}

sub _server_handle_conn {
	BugOUT(9,"JABus Server _handle_conn()->ENTER");
	my $self = $_[0];
	my $client_id = $_[1];
	my $client_conn_cond = $_[2];

	if ($client_conn_cond >= 'hup' or $client_conn_cond >= 'err') {
		BugOUT(9,"JABus Server _handle_conn()->ERRHUP [$client_conn_cond]!");
		$self->server_terminate_client_conn($client_id);
		BugOUT(9,"JABus Server _handle_conn()->DONE!");
		return 0;

	} elsif ( $client_conn_cond >= 'in' ) {
		if ($self->{'clients'}{$client_id}{'io_obj'}) {
			my $bytes_read = 0;
			my $in_data = "";

			BugOUT(9,"JABus Server _handle_conn()->READ starting");
			while($in_data !~ /\n$/s) {
				my $rb = sysread($self->{'clients'}{$client_id}{'io_obj'}, $in_data, 16384, length($in_data));
				last if ($rb == 0 && $in_data eq "");

				$bytes_read += $rb;

				BugOUT(9,"JABus Server _handle_conn()->BUFFER: '$in_data'");
			}

			BugOUT(9,"JABus Server _handle_conn()->READ completed, $bytes_read bytes read.");

			$self->{'total_bytes_read'} += $bytes_read;
			$self->{'clients'}{$client_id}{'bytes_read'} 
								+= $bytes_read;
if ($bytes_read > 0) {
foreach my $in_data_line (split(/\n/,$in_data)) {
	$in_data_line =~ s/^\s*//g;
	$in_data_line =~ s/\s*$//g;
	if ($in_data_line =~ /JAB/) {

			my $jData;
			eval {
				$jData = decode_json($in_data_line);
			} or warn("JABus Server _handle_conn()->DONE (Got some garbage instead of JSON!)");

			if ($jData) {
				if ($self->{'clients'}{$client_id}{'status'}{'auth'} eq 1) {
					if ($self->{'handle_in_solo'}) {
						BugOUT(9,"JABus::Socket _server_handle_conn()->Got JSON for solo!");
						
						$self->{'handle_in_solo'}->($jData->{'data'},$client_id,$self);


					} elsif ($self->{'handle_in_dispatch'}) {
						BugOUT(9,"JABus::Socket _server_handle_conn()->Got JSON for dispatch ($jData->{'disp_to'})!");

						if ($self->{'handle_in_dispatch'}{$jData->{'disp_to'}}) {
							$self->{'handle_in_dispatch'}{$jData->{'disp_to'}}($jData->{'data'},$client_id,$self);

						} else {
							die("Didn't we send over a list of valid options? WTF?");
						}
					}
				} else {
					my $server_handler_type;
					if ($self->{'handle_in_solo'}) {
						$server_handler_type = "solo";
					} elsif ($self->{'handle_in_dispatch'}) {
						$server_handler_type = "dispatch";
					} else {
						die("THIS SHOULD NEVER HAPPEN");
					}
					BugOUT(9,"JABus Server _handle_conn()-> PRE AUTH JSON...");
					
					if (($self->{'type'} eq "unix") and ($self->{'destination'} eq "local")) {
						# Accepting connections should probably move to a sub func 
						$self->{'clients'}{$client_id}{'status'}{'auth'} = 1;
						$self->{'clients'}{$client_id}{'declared_id'} = $jData->{'declare_id'};
						
						$self->{'clients'}{$client_id}{'handler_type'} = $jData->{'handler_type'};
						if ($jData->{'handler_type'} eq "dispatch") {
#							print "DUMPYO:\n",Dumper($jData->{'dispatch_list'}),"\n\n";
							$self->{'clients'}{$client_id}{'dispatch_list'} = $jData->{'dispatch_list'};
							# add sanitation of incomming list
						}
						if ($self->{'hooks'}{'on_server_client_auth_ok'}) {
							$self->{'hooks'}{'on_server_client_auth_ok'}($client_id,$self);
						}
						$self->_server_send($client_id,{
								'auth' => 1,
								'JAB' => 'auth',
								'server_declare_id' => $self->{'declare_id'},
								'server_handler_type' => $server_handler_type,
							});
					} else {
						if (length($jData->{'auth_key'}) > 30) {
							if ($jData->{'auth_key'} eq $self->{'_socket_auth_key'}) {
								BugOUT(8,"Auth SUCCESS!");
								$self->{'clients'}{$client_id}{'status'}{'auth'} = 1;
								$self->{'clients'}{$client_id}{'declared_id'} = $jData->{'declare_id'};
						
								$self->{'clients'}{$client_id}{'handler_type'} = $jData->{'handler_type'};
								if ($jData->{'handler_type'} eq "dispatch") {
#									print "DUMPYO:\n",Dumper($jData->{'dispatch_list'}),"\n\n";
									$self->{'clients'}{$client_id}{'dispatch_list'} = $jData->{'dispatch_list'};
									#FIXME Add sanitation of incomming list
								}
								if ($self->{'hooks'}{'on_server_client_auth_ok'}) {
									$self->{'hooks'}{'on_server_client_auth_ok'}($client_id,$self);
								}
								$self->_server_send($client_id,{
										'auth' => 1,
										'JAB' => 'auth',
										'server_declare_id' => $self->{'declare_id'},
										'server_handler_type' => $server_handler_type,
									});


							}
						} else {
							$self->server_terminate_client_conn($client_id);
							return 0;
						}
					}

				}
			} else {
				if ($self->{'clients'}{$client_id}{'status'}{'auth'} eq 1) {
					warn("JABus Server _handle_conn()-> Got the GARBAGE after ok auth.... ?!");
				} else {
					BugOUT(9,"JABus Server _handle_conn()->Probably something else tickeling our sockets...");
					$self->server_terminate_client_conn($client_id);
					BugOUT(8,"OR IS IT!?");
					return 0;
				}
			}

}}} else {
	warn("JABus Server _handle_conn()->DONE (client_fh read problem?!)");
	$self->server_terminate_client_conn($client_id);
	return 0;
}
			return 1;
		} else {
			warn("JABus Server _handle_conn()->DONE (client_fh problem?!)");
			$self->server_terminate_client_conn($client_id);
			return 0;
		}
	} else {
		warn("JABus Server _handle_conn()->DONE (Weird exception?!)");
		$self->server_terminate_client_conn($client_id);
		return 0;
	}
}

sub server_terminate_client_conn {
	# also serves as a general cleanup of failed or partialy initiated connections
	my $self = $_[0];
	my $client_id = $_[1];
	if ($self->{'clients'}{$client_id}) {
		if ($self->{'hooks'}{'on_server_terminate_client'}) {
			$self->{'hooks'}{'on_server_terminate_client'}($client_id,$self);
		}
		if ($self->{'clients'}{$client_id}{'watcher'}) {
			Glib::Source->remove($self->{'clients'}{$client_id}{'watcher'});
		}
		if ($self->{'clients'}{$client_id}{'io_obj'}) {
			if ($self->{'clients'}{$client_id}{'status'}{'auth'} eq 1) {
				# send clean termination message 
			}
			$self->{'clients'}{$client_id}{'io_obj'}->close;
			$self->{'clients'}{$client_id}{'io_obj'} = undef;
		}
		delete $self->{'clients'}{$client_id};
		BugOUT(8,"JABus Server _server_terminate_client_conn()-> CLEANUP DONE!");
	} else {
		BugOUT(8,"JABus Server _server_terminate_client_conn()-> ERR... Can't clean up a client id that does not exist!?! (Maybe we've taken care of it already?)");
	}

	return 1;
}

sub server_get_client_info {
	BugOUT(9,"JABus::Socket server_get_client_info()->ENTER");
	my $self = $_[0];
	my $client_id = $_[1];
	my $get_value = lc($_[2]);
	if ($self->{'clients'}{$client_id}) {
		$get_value =~ s/[\n\s]//g;
		if ($get_value =~ /^([a-z0-9\_]{3,})$/) {
			$get_value = $1;
			if ($self->{'clients'}{$client_id}{$get_value}) {
				BugOUT(9,"JABus::Socket server_get_client_info()-> Returning $client_id->$get_value");
				return $self->{'clients'}{$client_id}{$get_value};
			} else {
				BugOUT(9,"JABus::Socket server_get_client_info()-> ERR '$get_value' does not exist!");
				return 0;
			}
		} else {
			BugOUT(9,"JABus::Socket server_get_client_info()-> ERR'$get_value'?!");
			return 0;
		}
	} else {
		warn("JABus::Socket server_get_client_info()->Requesting info from invalid client_id? ($client_id)");
		return 0;
	}
	BugOUT(9,"JABus::Socket server_get_client_info()->DONE");
}

sub _client_handle_conn {
	BugOUT(9,"JABus::Socket _client_handle_conn()->ENTER");
	my $self = $_[0];
	my $connection_cond = $_[1];

	if ($connection_cond >= 'hup' or $connection_cond >= 'err') {
		if ($self->{'hooks'}{'on_client_errhup'}) {
			$self->{'hooks'}{'on_client_errhup'}($self);
		}
		BugOUT(9,"JABus::Socket _client_handle_conn()->ERRHUP!");
		$self->_client_terminate_conn();
		BugOUT(9,"JABus::Socket _client_handle_conn()->DONE!");
		return 0;
	} elsif ( $connection_cond >= 'in' ) {
		if ($self->{'the_socket'}) {
			my $bytes_read = 0;
			my $in_data = "";

			BugOUT(9,"JABus Server _handle_conn()->READ starting");
			while($in_data !~ /\n$/s) {
				my $rb = sysread($self->{'the_socket'}, $in_data, 16384, length($in_data));
				last if ($rb == 0 && $in_data eq "");

				$bytes_read += $rb;

				BugOUT(9,"JABus Server _handle_conn()->BUFFER: '$in_data'");
			}

			BugOUT(9,"JABus Server _handle_conn()->READ completed, $bytes_read bytes read.");


			$self->{'bytes_read'} += $bytes_read;

if ($bytes_read > 0) {
foreach my $in_data_line (split(/\n/,$in_data)) {
	$in_data_line =~ s/^\s*//g;
	$in_data_line =~ s/\s*$//g;
	if ($in_data_line =~ /JAB/) {

			my $jData;
			BugOUT(9,"JABus::Socket _client_handle_conn()->Received JSON: '$in_data_line'");

			eval {
				$jData = decode_json($in_data_line);
			} or warn("JABus  _client_handle_conn()->DONE (Got some garbage instead of JSON!)");

			if ($jData) {
#				print Dumper($jData);
				if ($self->{'status'}{'auth'} eq 1) {
					if ($self->{'handle_in_solo'}) {
						BugOUT(9,"JABus::Socket _client_handle_conn()->Got JSON for solo!");
						$self->{'handle_in_solo'}->($jData->{'data'}, sub {\$self->client_send($_[0],$_[1]);});
					} elsif ($self->{'handle_in_dispatch'}) {
						BugOUT(9,"JABus::Socket _client_handle_conn()->Got JSON for dispatch ($jData->{'disp_to'})!");
#						print "DISPATCHDATA:\n",Dumper($jData);
						if ($self->{'handle_in_dispatch'}{$jData->{'disp_to'}}) {
							$self->{'handle_in_dispatch'}{$jData->{'disp_to'}}($jData->{'data'}, sub {\$self->client_send($_[0],$_[1]);});
						} else {
							die("Didn't we send over a list of valid options? WTF?");
						}
					}
				} else {
					BugOUT(9,"JABus _client_handle_conn()-> PRE AUTH JSON...");
					if ($jData->{'auth'} eq 1) {
						BugOUT(9,"JABus _client_handle_conn()->AUTH OK! YAY!");
						$self->{'status'}{'auth'} = 1;
						$self->{'server_handler_type'} = $jData->{'server_handler_type'};
						$self->{'server_declared_id'} = $jData->{'server_declare_id'};

						if ($self->{'hooks'}{'on_ready'}) {
						# ADD SOME HOOK STUFF HERE?!
							$self->{'hooks'}{'on_ready'}($self->{'_socket_id'});# RETURN SOME OTHER VALUES?!
						}
					} else {
						BugOUT(9,"JABus _client_handle_conn()-> AUTH NOT OK?!?! WTF?!");
						$self->_client_terminate_conn();
					}
				}
			} 
			#or warn("JABus::Socket _client_handle_conn()->DONE (Got some garbage instead of JSON!)");
}}} else {
	warn("JABus::Socket _client_handle_conn()->DONE (client_fh problem?!)");
	$self->_client_terminate_conn();
	return 0;
}
			return 1;
		} else {
			warn("JABus::Socket _client_handle_conn()->DONE (client_fh problem?!)");
			$self->_client_terminate_conn();
			return 0;
		}
	} else {
		warn("JABus::Socket _client_handle_conn()->DONE (Weird exception?!)");
		$self->_client_terminate_conn();
		return 0;
	}
}

sub _client_terminate_conn {
	# also serves as a general cleanup of failed or partialy initiated connections
	my $self = $_[0];
	if ($self) {
		if ($self->{'watcher'}) {
			Glib::Source->remove($self->{'watcher'});
		}
		if ($self->{'the_socket'}) {
			if ($self->{'status'}{'auth'} eq 1) {
				# send clean termination message 
			}
			$self->{'the_socket'}->close;
			$self->{'the_socket'} = undef;
		}
		delete $arctica_core_object->{'aobj'}{'JABus'}{'Client'}{$self->{'_socket_id'}};
		undef $self;
	}

	return 1;
}

sub client_send {
	BugOUT(9,"JABus::Socket client_send()->ENTER");
	my $self = $_[0];
	if ($self->{'status'}{'auth'} eq 1) {
		if ($self->{'server_handler_type'} eq 'solo') {
			$self->_client_send({
				'data' => $_[1],
				'JAB' => 'data'
			});
		} elsif ($self->{'server_handler_type'} eq 'dispatch') {
			my $dispatch_to = lc($_[1]);
			if ($dispatch_to =~ /^([a-z0-9\_]{2,32})$/) {
				$dispatch_to = $1;
				$self->_client_send({
						'disp_to' => $dispatch_to,
						'data' => $_[2],
						'JAB' => 'data'
					});
			} else {
				die("JABus::Socket client_send() DISPATCH TO WHERE?!!?!");
			}
		} else {
			die("THIS SHOULD NEVER HAPPEN!?!");
		}
	}
	BugOUT(9,"JABus::Socket client_send()->DONE");
}



sub _client_send {
	BugOUT(9,"JABus::Socket _client_send()->ENTER");
	my $self = $_[0];
	my $data_to_send = $_[1];

	my $json_data;
	eval {
		$json_data = encode_json($data_to_send);
	} or warn("JABus::Socket _client_send() encode_json crapped it self!");

	if ($self->{'the_socket'} and $json_data) {
		my $bytes_written = syswrite($self->{'the_socket'}, "$json_data\n")
			 or warn("Error while trying to send to server ($!)");
		$self->{'bytes_written'}+=$bytes_written;
		BugOUT(8,"JABus::Socket _client_send()->JAB Sent...");
	}

	BugOUT(9,"JABus::Socket _client_send()->DONE");
}


sub get_socinf {
	my $self = $_[0];
	return ($self->{'_socket_id'},$self->{'_socket_auth_key'});
}


sub DESTROY {
	my $self = $_[0];
	if (($self->{'s_or_c'} eq "Server") and (-e "$self->{'the_socket_path'}.sock")) {
		BugOUT(9,"JABus::Socket $self->{'s_or_c'} DESTROY socket file exists: $self->{'the_socket_path'}.sock");
		unlink("$self->{'the_socket_path'}.sock") or warn("JABus::Socket DESTROY unable to unlink unix socket 'file'!");
	}

	if (-f "$self->{'the_socket_path'}.inf") {
		unlink("$self->{'the_socket_path'}.inf") or warn("JABus::Socket DESTROY unable to unlink socket inf file!");
	}

	warn("JABus::Socket Object $self->{'_socket_id'} ($self->{'s_or_c'}) DESTROYED");
	return 0;
}


1;
