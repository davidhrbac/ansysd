#!/usr/bin/perl

#------------------------------------------------------------------------------
# perl daemon to read ansys tasks spool stored in DB
# 2006/05/09 
#
# Author: David Hrbac <david.hrbac@vsb.cz>
# Patches and problem reports are welcome.
#
# The latest version of this program is available at:
#   http://www.pudu.cz/ansysd
#------------------------------------------------------------------------------

#use strict;

use DBI;
use Time::Local;
use POSIX qw(setsid strftime);
use Compress::Zlib;
use Unix::Syslog qw(:macros :subs);

##
# Signals to Trap and Handle
##
$SIG{'INT' } = 'interrupt';
$SIG{'HUP' } = 'interrupt';
$SIG{'ABRT'} = 'interrupt';
$SIG{'QUIT'} = 'interrupt';
$SIG{'TRAP'} = 'interrupt';
$SIG{'STOP'} = 'interrupt';
$SIG{'TERM'} = 'interrupt';

my $database = "ansys";
my $user = "ansys";
my $password ="heslobyloansys";
my $server = "localhost";

# Create debugging output - true: log to stderr; false: log to syslog/file
my $DEBUG = 0;
my $log_to_stderr=0;
my $do_syslog=1;
my $syslog_facility = 'daemon';   # Syslog facility as a string
           # e.g.: mail, daemon, user, local0, ... local7
my $syslog_priority = 'debug';  # Syslog base (minimal) priority as a string,
           # choose from: emerg, alert, crit, err, warning, notice, info, debug
my $eol = "\n";  # native record separator in files: LF or CRLF or even CR
my $myhostname = (POSIX::uname)[1];  # should be a FQDN !
my $myproduct_name = 'ansysd';
my $myversion_id = '1.0.0'; 
my $myversion_date = '20060514';
my $myversion = "$myproduct_name-$myversion_id ($myversion_date)";
my $myversion_id_numeric =  # x.yyyzzz, allows numerical comparision, like Perl $]
  sprintf("%8.6f", $1 + ($2 + $3/1000)/1000)
  if $myversion_id =~ /^(\d+)(?:\.(\d*)(?:\.(\d*))?)?(.*)$/;

my $folder_in="/fs10/in";
my $folder_out="/fs10/out";
my $folder_spool="/fs10/spool";

my $am_id=undef;
my $syslog_ident = 'ansysd';
my $SYSLOG_LEVEL = 'mail';

# Mostly for debugging and reporting purposes:
# Convert nonprintable characters in the argument
# to \[rnftbe], or \octal code, and '\' to '\\',
# and Unicode characters to \x{xxxx}, returning the sanitized string.
sub sanitize_str {
  my($str, $keep_eol) = @_;
  my(%map) = ("\r" => '\\r', "\n" => '\\n', "\f" => '\\f', "\t" => '\\t',
              "\b" => '\\b', "\e" => '\\e', "\\" => '\\\\');
  if ($keep_eol) {
    $str =~ s/([^\012\040-\133\135-\176])/  # and \240-\376 ?
              exists($map{$1}) ? $map{$1} :
                     sprintf(ord($1)>255 ? '\\x{%04x}' : '\\%03o', ord($1))/eg;
  } else {
    $str =~ s/([^\040-\133\135-\176])/      # and \240-\376 ?
              exists($map{$1}) ? $map{$1} :
                     sprintf(ord($1)>255 ? '\\x{%04x}' : '\\%03o', ord($1))/eg;
  }
  $str;
}

sub open_log() {
  # don't bother to skip opening the log even if $log_to_stderr (debug) is true
  if ($do_syslog) {
    my($id) = $syslog_ident; my($fac) = $syslog_facility;
    my($syslog_facility_num) = eval("LOG_\U$fac");
    $syslog_facility_num = LOG_DAEMON   if $syslog_facility_num !~ /^\d+\z/;
    openlog($id, LOG_PID | LOG_NDELAY, $syslog_facility_num);
    $current_syslog_ident = $id; $current_syslog_facility = $fac;
  } elsif ($logfile ne '') {
    $loghandle = IO::File->new($logfile,'>>')
      or die "Failed to open log file $logfile: $!";
    $loghandle->autoflush(1);
    if ($> == 0) {
      my($uid) = $daemon_user=~/^(\d+)$/ ? $1 : (getpwnam($daemon_user))[2];
      if ($uid) {
        chown($uid,-1,$logfile)
          or die "Can't chown logfile $logfile to $uid: $!";
      }
    }
  }
}

sub close_log() {
  if ($do_syslog) {
    closelog();
    $current_syslog_ident = $current_syslog_facility = undef;
  } elsif (defined($loghandle) && $logfile ne '') {
    $loghandle->close or die "Error closing log file $logfile: $!";
    $loghandle = undef;
  }
}

# Log either to syslog or to a file
sub write_log($$$;@) {
  my($level,$am_id,$errmsg,@args) = @_;
  $am_id = !defined $am_id ? '' : "($am_id) ";
  # treat $errmsg as sprintf format string if additional arguments provided
  if (@args && $errmsg=~/%/) { $errmsg = sprintf($errmsg,@args) }
  $errmsg = sanitize_str($errmsg);
# my($old_locale) = POSIX::setlocale(LC_TIME,"C");  # English dates required!
# if (length($errmsg) > 2000) {  # crop at some arbitrary limit (< LINE_MAX)
#   $errmsg = substr($errmsg,0,2000) . "...";
# }
  if ($do_syslog && !$log_to_stderr) {
    # never go below this priority level
#    my($prio) = $syslog_prio_name_to_num{uc($syslog_priority)};
    $level=0;
    $prio=LOG_NOTICE;
    if    ($level <= -3) { $prio = LOG_CRIT    if $prio > LOG_CRIT    }
    elsif ($level <= -2) { $prio = LOG_ERR     if $prio > LOG_ERR     }
    elsif ($level <= -1) { $prio = LOG_WARNING if $prio > LOG_WARNING }
    elsif ($level <=  0) { $prio = LOG_NOTICE  if $prio > LOG_NOTICE  }
    elsif ($level <=  2) { $prio = LOG_INFO    if $prio > LOG_INFO    }
    else                 { $prio = LOG_DEBUG   if $prio > LOG_DEBUG   }
    my($alert_mark) = $level < -1 ? '(!!) ' : $level < 0 ? '(!) ' : '';
    my($pre) = $alert_mark;
    my($logline_size) = 980;  # less than  (1023 - prefix)
    while (length($am_id)+length($pre)+length($errmsg) > $logline_size) {
      my($avail) = $logline_size - length($am_id . $pre . "...");
      syslog($prio, "%s", $am_id . $pre . substr($errmsg,0,$avail) . "...");
      $pre = $alert_mark . "...";  $errmsg = substr($errmsg, $avail);
    }
    if ($syslog_ident ne $current_syslog_ident ||
        $syslog_facility ne $current_syslog_facility) {
      close_log()  if !defined($current_syslog_ident) &&
                      !defined($current_syslog_facility);
      open_log();
    }
    syslog($prio, "%s", $am_id . $pre . $errmsg);
  } else {
    my($prefix) = sprintf("%s %s %s[%s]: ",      # prepare syslog-alike prefix
           strftime("%b %e %H:%M:%S",localtime), $myhostname, $myname, $$);
    if (defined $loghandle && !$log_to_stderr) {
      lock($loghandle);
      seek($loghandle,0,2) or die "Can't position log file to its tail: $!";
      $loghandle->print($prefix, $am_id, $errmsg, $eol)
        or die "Error writing to log file: $!";
      unlock($loghandle);
    } else {
      print STDERR $prefix, $am_id, $errmsg, $eol
        or die "Error writing to STDERR: $!";
    }
  }
# POSIX::setlocale(LC_TIME, $old_locale);
}

##
# Interrupt: Simple interrupt handler
##
sub interrupt {
  write_log(0,$am_id,"Killed by @_");
	exit;
}

$DEBUG=1      if $ARGV[0] eq 'debug';
$log_to_stderr=1 if $DEBUG;
$daemonize = $DEBUG ? 0 : 1;

POSIX::setlocale(LC_TIME,"C");  # English dates required in syslog and rfc2822!

push(@config_files, '/etc/amavisd.conf')  if !@config_files;

open_log();

$myname = $0;
my($msg) = "Starting $myname at $myhostname $myversion";
$msg .= ", eol=\"$eol\""            if $eol ne "\n";
$msg .= ", Unicode aware"           if $unicode_aware;
$msg .= ", LC_ALL=$ENV{LC_ALL}"     if $ENV{LC_ALL}   ne '';
$msg .= ", LC_TYPE=$ENV{LC_TYPE}"   if $ENV{LC_TYPE}  ne '';
$msg .= ", LC_CTYPE=$ENV{LC_CTYPE}" if $ENV{LC_CTYPE} ne '';
$msg .= ", LANG=$ENV{LANG}"         if $ENV{LANG}     ne '';
write_log(0,$am_id,$msg);

write_log(0,$am_id,"Perl version               %s", $]);

# report versions of some modules
for my $m (
        sort map { s/\.pm\z//; s[/][::]g; $_ } grep { /\.pm\z/ } keys %INC) {
  next  if !grep { $_ eq $m } qw(Amavis::Conf
    Archive::Tar Archive::Zip Compress::Zlib Convert::TNEF Convert::UUlib
    MIME::Entity MIME::Parser MIME::Tools Mail::Header Mail::Internet
    Mail::ClamAV Mail::SpamAssassin Mail::SpamAssassin::SpamCopURI URI
    Razor2::Client::Version Mail::SPF::Query Digest::MD5 Authen::SASL
    IO::Socket::INET6 Net::DNS Net::SMTP Net::Cmd Net::Server Net::LDAP
    DBI DBD::mysql DBD::Pg DBD::SQLite BerkeleyDB DB_File
    SAVI Unix::Syslog Time::HiRes);
  write_log(0,$am_id, "Module %-19s %s", $m, $m->VERSION || '?');
}

if ($daemonize)
{
  defined(my $pid = fork)   or die "Can't fork: $!";
  exit if $pid;
  setsid                    or die "Can't start a new session: $!";
}

write_log(2,$am_id,"Connecting to db");
my $db = DBI -> connect ("DBI:mysql:$database:$server",$user,$password,{ RaiseError => 1});
write_log(2,$am_id,"Connected to db");


while (1)
{
  write_log(2,$am_id,"Selecting record");
  my $query = $db ->prepare ("select * from spool where status=1 order by date_ins asc limit 1;");
  $query->execute;
  write_log(2,$am_id,"Selected record");
  if (my @row_ary = $query->fetchrow_array())
  {
    write_log(2,$am_id,"Got record, task id: #%s",$row_ary[0]);
    my $query_change_status = $db ->prepare ("update spool set status=2, date_start=now() where id=$row_ary[0];");
    $query_change_status->execute;
    $time=strftime("%d.%m.%y %H:%M:%S", localtime(time));

    system("echo \"\" | mail -s \"FS10: $time - bylo zapocato zpracovani vasi ulohy #$row_ary[0]...\" $row_ary[2]");
    
    if (-e "$folder_spool/$row_ary[0]" && -e "$folder_spool/$row_ary[0]/$row_ary[3]")
    {  
      write_log(2,$am_id,"File exists: %s/%s/%s",$folder_spool,$row_ary[0],$row_ary[3]);
      chdir "$folder_spool/$row_ary[0]"  or die "can't chdir to $!";  
      write_log(2,$am_id,"Chdired to: %s/%s",$folder_spool,$row_ary[0]);
      
      write_log(0,$am_id,"Trying to run: ansys100 -P ANSYSRF -b < %s",$row_ary[3]);
      $ansys_log = `ansys100 -P ANSYSRF -b < $row_ary[3]`;

      my($elapsed_time) = $ansys_log=~ /Elapsed Time.*\) =\s*(\d+\.\d+)/;
      write_log(0,$am_id,"Computation finished, elapsed time %s",$elapsed_time);

      $ansys_log_bz=Compress::Zlib::memGzip($ansys_log);

      open O, ">output.log.gz"; 
      print O $ansys_log_bz;
      close O;

      $mv=`mv $folder_spool/$row_ary[0] $folder_out/$row_ary[0]_$row_ary[4]`;
      
      write_log(2,$am_id,"Trying to run: update spool set status=3, log='', time_elapsed=%s where id=%s;", $elapsed_time,$row_ary[0]);  
      my $query_change_status = $db ->prepare ("update spool set status=?, log=?, time_elapsed=? where id=$row_ary[0];");
      $query_change_status->execute(3,$ansys_log_bz,$elapsed_time);
    }
    else
    {
      write_log(2,$am_id,"Unable to find file: %s/%s/%s",$folder_spool,$row_ary[0],$row_ary[3]);
      $ansys_log_bz=Compress::Zlib::memGzip("Soubory neexistují");
      my $query_change_status = $db ->prepare ("update spool set status=?, log=? where id=$row_ary[0];");
      $query_change_status->execute(4,$ansys_log_bz);
    }

    $time=strftime("%d.%m.%y %H:%M:%S", localtime(time));
    system("echo \"\" | mail -s \"FS10: $time - bylo ukonceno zpracovani vasi ulohy #$row_ary[0]...\" $row_ary[2]");

    sleep(1);
  }
  else
  {
    write_log(2,$am_id,"Nothing to do, going to sleep 10 sec");
    sleep(10);
  }
}

1;
