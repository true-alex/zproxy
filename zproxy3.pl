use warnings;
use strict;
use POSIX;
use IO::Socket;
use IO::Select;
use Socket;
use Fcntl;
use Carp;
my $daemon = 0;

my $cnv_ports    = [25000];
my $client_ports = [25001];

my $clients_count = 16;
my $pid_file = undef;
my $timeout = 800;
my $connects_per_ip=0;

sub Opts {
	my ($opts, %opts) = @_;
	my $tags = $opts{tags} || { };
	my $allow_space = $opts{allow_space} ? 1 : 0;
	my @args;
	my $cnt = 0;
	my $sep_opts=''; # Добавляем поддержку записи параметров через пробел -p 1
	foreach ( @ARGV ) {
		if ( $opts{tail} && ($_ eq $opts{tail}) ) { # Флаг после которого опции перестаём разбирать
			if ( $sep_opts ne '' ) {
				if ( 'ARRAY' eq ref $opts->{$sep_opts} ) {
					$opts->{$sep_opts}[-1] = undef;
				} else {
					$opts->{$sep_opts} = undef;
				}
			}
			$sep_opts='';
			$opts->{tail} = [ @ARGV[ $cnt + 1 .. $#ARGV ] ];
			return @args
		}
		if ( /^\-\-(\S+?)(?:(=)(.*))?$/ ) { # Поддержка параметров --param_name=param_value а так же сокращённой записи --param_name и --param_name=
			if ( $sep_opts ne '' ) {
				if ( 'ARRAY' eq ref $opts->{$sep_opts} ) {
					$opts->{$sep_opts}[-1] = undef;
				} else {
					$opts->{$sep_opts} = undef;
				}
			}
			$sep_opts='';
			if ( exists $opts->{$1} ) {
				$opts->{$1} = [$opts->{$1}] unless 'ARRAY' eq ref $opts->{$1};
				push @{$opts->{$1}}, $3//((($2//'') eq '=')?'':undef);
			} else {
				$opts->{$1} = $3//((($2//'') eq '=')?'':undef);
			}
			if ( ( ($2//'') ne '=') && !defined($3) && $opts{allow_space}) {
				$sep_opts = $1;
			}
		} elsif( /^\-(\S)(.*)/ ) {
			if ( $sep_opts ne '' ) {
				if ( 'ARRAY' eq ref $opts->{$sep_opts} ) {
					$opts->{$sep_opts}[-1] = undef;
				} else {
					$opts->{$sep_opts} = undef;
				}
			}
			$sep_opts='';
			if ( $1 eq ':' ) {
				my $ops = $2;
				if($ops =~ s/:$//){
					croak "Bad option tag \"$ops\"\n"	unless exists $tags->{$ops};
					$ops = $tags->{$ops};
				}
				foreach ( grep{!exists $opts->{$_}} split //, $ops ) {
					if ( exists $opts->{$_} ) {
						$opts->{$_} = [$opts->{$_}] unless 'ARRAY' eq ref $opts->{$_};
						push @{$opts->{$_}}, '';
					} else {
						$opts->{$_} = '';
					}
				}
			} else {
				$sep_opts=$1 if $allow_space && ($2 eq '');
				if ( exists $opts->{$1} ) {
					$opts->{$1} = [$opts->{$1}] unless 'ARRAY' eq ref $opts->{$1};
					push @{$opts->{$1}}, $2//undef;
				} else {
					$opts->{$1} = $2//undef;
				}
			}
		} else {
			if ( $sep_opts ne '' ) {
				if ( 'ARRAY' eq ref $opts->{$sep_opts} ) {
					$opts->{$sep_opts}[-1] = $_;
				} else {
					$opts->{$sep_opts} = $_;
				}
				$sep_opts='';
			} else {
				push @args, $_;
			}
		}
		$cnt++;
	}
	if ( $sep_opts ne '' ) {
		if ( 'ARRAY' eq ref $opts->{$sep_opts} ) {
			$opts->{$sep_opts}[-1] = undef;
		} else {
			$opts->{$sep_opts} = undef;
		}
	}
	return @args
}

my $args={};
Opts($args);
if ( $args->{converters} ) {
	$args->{converters} = [$args->{converters}] unless 'ARRAY' eq ref $args->{converters};
	$cnv_ports = [ grep {$_} map {$_ + 0;} @{$args->{converters}} ];
}
if ( $args->{clients} ) {
	$args->{clients} = [$args->{clients}] unless 'ARRAY' eq ref $args->{clients};
	$client_ports = [ grep {$_} map {$_ + 0;} @{$args->{clients}} ];
}

$daemon=1 if exists($args->{d}) && ($args->{d} eq '' || $args->{d});
$timeout=1*($args->{timeout}||900)+0 if defined($args->{timeout}) && 1*($args->{timeout}||900)+0 > 10;
$pid_file=$args->{pid_file};  
$connects_per_ip=1*((+($args->{per_ip}//0))||0);

die "Converter port not defined\n" unless scalar @$cnv_ports;
die "Converter port(s) ".join(", ",grep { $_ <= 1024 || $_ >= 65000 } @$cnv_ports)." is not valid ( 1024 < ... < 65000 ) \n" if scalar(grep { $_ <= 1024 || $_ >= 65000 } @$cnv_ports);
die "Clinet port not defined\n" unless scalar @$client_ports;
die "Clinet port(s) ".join(", ",grep { $_ <= 1024 || $_ >= 65000 } @$client_ports)." is not valid ( 1024 < ... < 65000 ) \n" if scalar(grep { $_ <= 1024 || $_ >= 65000 } @$client_ports);

sub save_to_file {
	my ($filename,$data) = @_;
	open(FH, '>', $filename) or die $!;
	print FH $data;
	close(FH);
}

if ( $daemon ) {
  ###Создаем процесс-демон###
  my $pid= fork();
  if($pid){
    save_to_file($pid_file,"$pid");
    exit();
  }
  die "Couldn't fork: $! " unless defined($pid);
  ###Создаем связь с новым терминалом###
  POSIX::setsid() or die "Can't start a new session $!";
	if ( POSIX::getuid() == 0 ) {
		POSIX::setuid(65534);
	}
}
my %socket_hashes = ();
my $select = IO::Select->new();

# Открываем сокеты
foreach my $port (@$cnv_ports) {
	my $socket = IO::Socket::INET->new(LocalPort => $port,
	                                   Listen    => $clients_count,
	                                   Reuse 	 => 1 )
	  or die "Can't make server socket: $@\n";
    $socket_hashes{$socket} = {
    	type   => 'cnv',
    	link   => $socket,
    	server => 1,
    	ip     => '0.0.0.0',
    	port   => $port,
    	ipport => "0.0.0.0:${port}",
    	active => 1,
    	outbuffer => '',
    };
	nonblock($socket);
	$select->add($socket);
}
foreach my $port (@$client_ports) {
	my $socket = IO::Socket::INET->new(LocalPort => $port,
	                                   Listen    => $clients_count,
	                                   Reuse 	 => 1 )
	  or die "Can't make server socket: $@\n";
    $socket_hashes{$socket} = {
    	type   => 'client',
    	link   => $socket,
    	server => 1,
    	ip     => '0.0.0.0',
    	port   => $port,
    	ipport => "0.0.0.0:${port}",
    	active => 1,
    	outbuffer => '',
    };
	nonblock($socket);
	$select->add($socket);
}

# Устанавливаем обработчик завершения процесса и закрытия сокетов
sub signal_handler {
	foreach my $serv ( values %socket_hashes ) {
		next unless $serv->{server};
	    close($serv->{link});
	}
    exit 1;
}
$SIG{INT}= $SIG{TERM} = \&signal_handler;

my $cmdAdvanced = "\xFF\xFA\x2C\x01\x00\x03\x84\x00\xFF\xF0";
my $cmdInfo     = "\xC8\x0D";  
my $shutUp = sub {die "he must be silent now!"};

# Начать с пустыми буферами
my @need_close=();
my %connects_per_ip=();
my %cnv_list = (); # Ключем является key для конвертера, значением массив указателей на элементы в socket_hash;

# Главный цикл: проверка чтения/принятия, проверка записи,
# проверка готовности к обработке
while (1) {
    my $client;
    my $rv;
    my $data;

    # Есть ли что-нибудь для чтения или подтверждения?
    my $now=time;
    foreach $client ($select->can_read(0.2)) {
        if ( exists($socket_hashes{$client}) && $socket_hashes{$client}{server} ) {
            # Принять новое подключение
            my $server = $client;
            $client = $server->accept();
           	my ($prip)=getpeername($client);
           	my ($port,$ipaddr)=unpack_sockaddr_in($prip);
			$ipaddr = inet_ntoa($ipaddr);
            $socket_hashes{$client} = {
		    	type   => $socket_hashes{$server}{type},
		    	link   => $client,
		    	client => 1,
		    	ip     => $ipaddr,
		    	ipport => "$ipaddr:$port",
		    	active => $now-$timeout+10, # Записываем последнее время активности (для клиентов, для серверов не используется)
		    	cb     => ($socket_hashes{$server}{type} eq 'client' ? sub {
		    		my ( $shc, $data ) = @_;
					if ( $data =~ /^LIST\s([0-9A-F]{2,8})\b/ ) {
						$shc->{outbuffer} .="LIST ".join('', (map {sprintf("%04X ",$_)} keys(%{($cnv_list{$1}//{})})))."\x0D";
					} elsif ( $data =~ /^CONN\s([0-9A-F]{4})\s([0-9A-F]{2,8})\b/ ) {
						my $id=hex($1);
						my $key=$2;
						if ( $shc->{binded} ) {
							if ( $shc->{binded}{cnv_key} ne $key || $shc->{binded}{cnv_id} ne $id ) {
								print STDERR "Client disconnected from $shc->{binded}{cnv_id}\n";
								delete $shc->{binded}{binded};
								delete $shc->{binded};
							} else {
								delete $shc->{binded}{binded};
								delete $shc->{binded};
							}
						}
						if ( defined $cnv_list{$key}{$id} ) {
							if ( exists $cnv_list{$key}{$id}{binded} ) {
								$shc->{outbuffer}.="BUSY";
							} else {
								$shc->{binded} = $cnv_list{$key}{$id};
								$shc->{binded}{binded} = $shc;
								$shc->{outbuffer}.="OK";
							}
						} else {
							$shc->{outbuffer}.="NONE";
						}
					} else {
						die "I did not understand him!";
					}
		    	} : $shutUp), # Колбек вызывается всякий раз когда приходят данные для которых нет собственного обработчика
            };

			# Если установлено ограничение на число соединений с одного IP блокируем соединение
			$connects_per_ip{$socket_hashes{$client}{ip}}=($connects_per_ip{$socket_hashes{$client}{ip}}//0)+1;
            if ( $connects_per_ip && $connects_per_ip{$socket_hashes{$client}{ip}} > $connects_per_ip ) {
                $connects_per_ip{$socket_hashes{$client}{ip}}--;
				delete $socket_hashes{$client};
                close $client;
                next;
            } else {
	            $select->add($client);
    	        nonblock($client);
	        }
	        if ( $socket_hashes{$client}{type} eq 'cnv' ) {
	        	$socket_hashes{$client}{need_adv} = $now+1; # Через секунду пошлём строку перехода в режим Advanced, если он сам себя не проявит
	        } 
        } elsif ( exists($socket_hashes{$client}) && $socket_hashes{$client}{client} ) {
            # Прочитать данные
            $data = '';
            $rv   = $client->recv($data, POSIX::BUFSIZ, 0);

            if ( !defined($rv) || !length($data) ) {
                $connects_per_ip{$socket_hashes{$client}{ip}}--;
                $select->remove($client);
                delete $socket_hashes{$client}{binded}{binded} if $socket_hashes{$client}{binded};
                if ( defined ($socket_hashes{$client}{cnv_id}) && defined($socket_hashes{$client}{cnv_key}) ) {
					delete $cnv_list{$socket_hashes{$client}{cnv_key}}{$socket_hashes{$client}{cnv_id}};
					delete $cnv_list{$socket_hashes{$client}{cnv_key}} unless scalar keys %{$cnv_list{$socket_hashes{$client}{cnv_key}}};
				}
				delete $socket_hashes{$client};
                close $client;
                next;
            }
            $socket_hashes{$client}{active} = $now;
			next if $data =~ /^\xFF/ && $socket_hashes{$client}{type} eq 'client'; # Это команда NVT, при общении с клиентом мы их игнорируем
			if ( defined $socket_hashes{$client}{binded} ) {
				$socket_hashes{$client}{binded}{outbuffer} .= $data;
			} else {
				eval{$socket_hashes{$client}{cb}($socket_hashes{$client},$data);};
				if ( $@ ) { # Если callback вернул ошибку, то закрываем соединение
	                $connects_per_ip{$socket_hashes{$client}{ip}}--;
	                $select->remove($client);
	                delete $socket_hashes{$client}{binded}{binded} if $socket_hashes{$client}{binded};
	                if ( defined ($socket_hashes{$client}{cnv_id}) && defined($socket_hashes{$client}{cnv_key}) ) {
						delete $cnv_list{$socket_hashes{$client}{cnv_key}}{$socket_hashes{$client}{cnv_id}};
						delete $cnv_list{$socket_hashes{$client}{cnv_key}} unless scalar keys %{$cnv_list{$socket_hashes{$client}{cnv_key}}};
					}
					delete $socket_hashes{$client};
	                close $client;
	                next;
				}
			}
        } else { #!exists($socket_hashes{$client})
			close $client; # Закрываем неизвестное соединение
        }
    }
    # Закрываем все соединения которые считаем необходимым закрыть, если они не проявили активность
    if ( scalar @need_close ) {
    	foreach $client (@need_close) {
	    	if ( $socket_hashes{$client}{active} < $now - $timeout ) {
                $connects_per_ip{$socket_hashes{$client}{ip}}--;
                delete $socket_hashes{$client}{binded}{binded} if $socket_hashes{$client}{binded};
                if ( defined ($socket_hashes{$client}{cnv_id}) && defined($socket_hashes{$client}{cnv_key}) ) {
					delete $cnv_list{$socket_hashes{$client}{cnv_key}}{$socket_hashes{$client}{cnv_id}};
					delete $cnv_list{$socket_hashes{$client}{cnv_key}} unless scalar keys %{$cnv_list{$socket_hashes{$client}{cnv_key}}};
				}
				delete $socket_hashes{$client};
                $select->remove($client);
                close $client;
    		}
    	}
    	@need_close=();
    }

    foreach $client ($select->can_write(0.2)) {
    	if ( $socket_hashes{$client}{client} && $socket_hashes{$client}{active} < $now - $timeout ) { # Клиент давно ничего не пишет, отрубаем
    		push @need_close, $client;
    	}
    	if ( defined $socket_hashes{$client}{need_adv} && $socket_hashes{$client}{need_adv} < $now ) { # Это соединение от конвертера, которое только что открыли и ничего в него не написали
    		# Отправим в него переключение врежим Advanced
    		$socket_hashes{$client}{outbuffer} .= $cmdAdvanced;
			$socket_hashes{$client}{need_req} = $now+1;
			delete $socket_hashes{$client}{need_adv};
		}
    	if ( defined $socket_hashes{$client}{need_req} && $socket_hashes{$client}{need_req} < $now ) { # Это соединение от конвертера, в которое мы отправили команду активации Advanced протокола
    		$socket_hashes{$client}{outbuffer} .= $cmdInfo;
			delete $socket_hashes{$client}{need_req};
			$socket_hashes{$client}{cb} = sub {
				my ( $shc,$data ) = @_;
				if ( $data =~ /^Z397-(?:WEB|IP) S\/N:(\d+),Mode:1,([0-9A-F]{1,8})/ ) {
					$shc->{cnv_id} = $1;
					$shc->{cnv_key} = $2;
					$cnv_list{$shc->{cnv_key}}//={} unless defined $cnv_list{$shc->{cnv_key}};
					if ( defined $cnv_list{$shc->{cnv_key}}{$shc->{cnv_id}} ) { # Уже существует конвертер так себя называющий
						if ( exists $cnv_list{$shc->{cnv_key}}{$shc->{cnv_id}}{binded}  ) { # И он с кем-то слинкован, то перекидываем линк на себя
							# перекидываем ссылку 
							$shc->{binded} = $cnv_list{$shc->{cnv_key}}{$shc->{cnv_id}}{binded};
							$shc->{binded}{binded} = $shc;
							delete $cnv_list{$shc->{cnv_key}}{$shc->{cnv_id}}{binded};
							$cnv_list{$shc->{cnv_key}}{$shc->{cnv_id}}{active} = $now-$timeout; # Говорим что это соединение уже давн оне активно
						}
						delete $cnv_list{$shc->{cnv_key}}{$shc->{cnv_id}}{cnv_id};
						delete $cnv_list{$shc->{cnv_key}}{$shc->{cnv_id}}{cnv_key};
					}
					$cnv_list{$shc->{cnv_key}}{$shc->{cnv_id}}=$shc;
					$shc->{cb} = $shutUp;
				} else {
					die "I did not understand him!";
				}
			};
		}
        # Пропустить этот клиент, если нам нечего сказать
        next unless $socket_hashes{$client}{outbuffer};

        $rv = $client->send($socket_hashes{$client}{outbuffer}, 0);
        unless (defined $rv) {
            # Пожаловаться, но следовать дальше.
            warn "I was told I could write, but I can't.\n";
            next;
        }
        if ($rv == length $socket_hashes{$client}{outbuffer} || $! == POSIX::EWOULDBLOCK)
        {
            substr($socket_hashes{$client}{outbuffer}, 0, $rv) = '';
            $socket_hashes{$client}{outbuffer}='';
        } else {
            # Не удалось записать все данные и не из-за блокировки.
            # Очистить буферы и следовать дальше.
            $connects_per_ip{$socket_hashes{$client}{ip}}--;
            $select->remove($client);
            delete $socket_hashes{$client}{binded}{binded} if $socket_hashes{$client}{binded};
            if ( defined ($socket_hashes{$client}{cnv_id}) && defined($socket_hashes{$client}{cnv_key}) ) {
				delete $cnv_list{$socket_hashes{$client}{cnv_key}}{$socket_hashes{$client}{cnv_id}};
				delete $cnv_list{$socket_hashes{$client}{cnv_key}} unless scalar keys %{$cnv_list{$socket_hashes{$client}{cnv_key}}};
			}
			delete $socket_hashes{$client};
            close $client;
            next;
        }
    }
}

# nonblock($socket) переводит сокет в неблокирующий режим
sub nonblock {
    my $socket = shift;
    my $flags;

    $flags = fcntl($socket, F_GETFL, 0)
            or die "Can't get flags for socket: $!\n";
    fcntl($socket, F_SETFL, $flags | O_NONBLOCK)
            or die "Can't make socket nonblocking: $!\n";
}
