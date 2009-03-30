package Parallel::Fork::BossWorkerAsync;
use strict;
use warnings;
use Carp;
use Data::Dumper qw( Dumper );
use Socket       qw( AF_UNIX SOCK_STREAM PF_UNSPEC );
use Fcntl        qw( F_GETFL F_SETFL O_NONBLOCK );
use POSIX        qw( WNOHANG WIFEXITED EINTR EWOULDBLOCK );
use IO::Select ();

our @ISA = qw();
our $VERSION = '0.01';

# -----------------------------------------------------------------
sub new {
  my ($class, %attrs)=@_;
  my $self = {
    work_handler    => $attrs{work_handler},                # required
    init_handler    => $attrs{init_handler}   || undef,     # optional
    result_handler  => $attrs{result_handler} || undef,     # optional
    worker_count    => $attrs{worker_count}   || 3,         # optional, how many child workers
    global_timeout  => $attrs{global_timeout} || 0,         # optional, in seconds, 0 is unlimited
    msg_delimiter   => $attrs{msg_delimiter}  || "\0\0\0",  # optional, may not appear in data
    shutting_down   => 0,
    force_down      => 0,
    pending         => 0,
    result_stream   => '',
    result_queue    => [],
    job_queue       => [],
  };
  bless($self, ref($class) || $class);

  croak("Parameter 'work_handler' is required") if ! defined($self->{work_handler});

  # Start the "boss" process, which will start the workers
  $self->start_boss();

  return $self;
}

# -----------------------------------------------------------------
sub serialize {
  my ($self, $ref)=@_;
  local $Data::Dumper::Deepcopy = 1;
  local $Data::Dumper::Indent = 0;
  local $Data::Dumper::Purity = 1;
  return Dumper($ref) . $self->{msg_delimiter};
}

# -----------------------------------------------------------------
sub deserialize {
  my ($self, $data)=@_;
  $data = substr($data, 0, - length($self->{msg_delimiter}));
  my $VAR1;
  my $ref = eval($data);
  if ($@) {
    confess("failed to deserialize: $@");
  }
  return $ref;  
}

# -----------------------------------------------------------------
# Pass one or more hashrefs for the jobs.
# Main app sends jobs to Boss.
sub add_work {
  my ($self, @jobs)=@_;
  $self->blocking($self->{boss_socket}, 1);
  while (@jobs) {
    my $job = shift(@jobs);
    my $n = syswrite( $self->{boss_socket}, $self->serialize($job) );
    croak("syswrite: $!") if ! defined($n);
    $self->{pending} ++;
  }
}

# -----------------------------------------------------------------
# Syntactic nicety
sub get_result_nb {
  my ($self)=@_;
  return $self->get_result(blocking => 0);
}

# -----------------------------------------------------------------
# Main app gets a complete, single result from Boss.
# If defined, result_handler fires here.
# Return is result of work_handler, or result_handler (if defined),
# or {} (empty hash ref).
# Undef is returned if socket marked nonblocking and read would have
# blocked.
sub get_result {
  my ($self, %args)=@_;
  $args{blocking} = 1 if ! defined($args{blocking});
  carp("get_result() when no results pending") if ! $self->pending();
  
  if ( ! @{ $self->{result_queue} }) {
    $self->blocking($self->{boss_socket}, $args{blocking});
    $self->read($self->{boss_socket}, $self->{result_queue}, \$self->{result_stream}, 'app');
  
    # Handle nonblocking case
    if ( ! $args{blocking}  &&  ! @{ $self->{result_queue} }) {
      return undef;
    }
  }

  $self->{pending} --;
  if ($self->{pending} == 0  &&  $self->{shutting_down}) {
    close($self->{boss_socket});
  }
  my $ref = $self->deserialize( shift( @{ $self->{result_queue} } ) );
  my $retval = $self->{result_handler} ? $self->{result_handler}->($ref) : $ref;
  $retval = {} if ! defined($retval);
  return $retval;
}

# -----------------------------------------------------------------
# Main app calls to see if there are submitted jobs for which no
# response has been collected.  It doesn't mean the responses are
# ready yet.
sub pending {
  my ($self)=@_;
  return $self->{pending};
}

# -----------------------------------------------------------------
# App tells boss to shut down by half-close.
# Boss then finishes work in progress, and eventually tells
# workers to exit.
# Boss sends all results back to app before exiting itself.
# Note: Boss won't be able to close cleanly if app ignores
# final reads...
# args: force => 0,1  defaults to 0
sub shut_down {
  my ($self, %args)=@_;
  $args{force} ||= 0;
  $self->{shutting_down} = 1;
  
  if ($args{force}) {
    # kill boss pid
    kill(9, $self->{boss_pid});
  } elsif ($self->pending()) {
    shutdown($self->{boss_socket}, 1);
  } else {
    close($self->{boss_socket});
  }
}

# -----------------------------------------------------------------
sub reaper {
  while ( (my $pid = waitpid(-1, WNOHANG)) > 0) {
    #carp("Child $pid exited") if WIFEXITED($?);
  }
  $SIG{CHLD} = \&reaper;
}

# -----------------------------------------------------------------
# Make socket blocking/nonblocking
sub blocking {
  my ($self, $socket, $makeblocking)=@_;
  my $flags = fcntl($socket, F_GETFL, 0)
    or croak("fcntl failed: $!");
  my $blocking = ($flags & O_NONBLOCK) == 0;
  if ($blocking  && ! $makeblocking) {
    $flags |= O_NONBLOCK;
  } elsif (! $blocking && $makeblocking) {
    $flags &= ~O_NONBLOCK;
  } else {
    # do nothing
    return $blocking;
  }
  
  fcntl($socket, F_SETFL, $flags)
    or croak("fcntl failed: $!");
  return $blocking;
}

# -----------------------------------------------------------------
sub start_boss {
  my ($self)=@_;
  eval {
    $SIG{CHLD} = \&reaper;
    my ($b1, $b2);
    socketpair($b1, $b2, AF_UNIX, SOCK_STREAM, PF_UNSPEC)
      or die("socketpair: $!");

    if (my $pid = fork()) {
      confess("fork failed: $!") if $pid < 0;
      # Application (parent)
      $self->{boss_pid} = $pid;

      # App won't write to, or read from itself.
      close($b2);
      $self->{boss_socket} = $b1;
      
    } else {
      # Manager aka Boss (child)
      # Boss won't write to, or read from itself.
      close($b1);
      
      $self->{app_socket} = $b2;
      
      # Make nonblocking
      $self->blocking( $self->{app_socket}, 0 );
      open(STDIN, '/dev/null');
      
      $self->start_workers();
      $self->boss_loop();
      exit;
    }
  };
  if ($@) {
    croak($@);
  }
}

# -----------------------------------------------------------------
sub start_workers {
  my ($self)=@_;
  eval {
    $SIG{CHLD} = \&reaper;
    
    for (1 .. $self->{worker_count}) {
      my ($w1, $w2);
      socketpair($w1, $w2, AF_UNIX, SOCK_STREAM, PF_UNSPEC)
        or die("socketpair: $!");
      
      if (my $pid = fork()) {
        confess("fork failed: $!") if $pid < 0;
        # Boss (parent)
        close($w2);
        $self->{workers}->{ $w1 } = { pid => $pid, socket => $w1 };

        # Make nonblocking
        $self->blocking( $w1, 0 );
        
      } else {
        # Worker (child)
        close($self->{app_socket});
        delete($self->{workers});
        close($w1);
        $self->{socket} = $w2;
        open(STDIN, '/dev/null');
      
        $self->worker_loop();
        exit;
      }
    }
  };
  if ($@) {
    croak($@);
  }
}

# -----------------------------------------------------------------
# Boss process; have an open socket to the app, and one to each worker.
# Loop select(), checking for read and write on app socket, and read
# on working children, and write on idle children.
# Keep track of idle vs. working children.
# When receive a shutdown order from the app, keep looping until the
# job queue is empty, and all results have been retrieved (all
# children will now be idle.)  Then close the worker sockets.
# They'll be reading, and will notice this and exit.
# Don't deserialize any data.  Just look for the delimiters to know
# we're processing whole records.
#

sub boss_loop {
  my ($self)=@_;
  eval {
    # handy
    my $workers = $self->{workers};
    
    # All workers start out idle
    for my $s (keys(%$workers)) {
      $workers->{ $s }->{idle} = 1;
    }
    
    while ( 1 ) {
      # When to exit loop?
      #   shutting_down = 1
      #   job_queue empty
      #   all workers idle, and no partial jobs
      #   result_queue empty
      if ($self->{shutting_down}  &&
          ! @{ $self->{job_queue} }  &&
          ! @{ $self->{result_queue} } ) {
        my $busy=0;
        my $partials = 0;
        for my $s (keys(%$workers)) {
          if ( ! $workers->{ $s }->{idle}) {
            $busy ++;
            last;
          } elsif (exists($workers->{ $s }->{partial_job})) {
            $partials ++;
            last;
          }
        }
        if ( ! $busy  &&  ! $partials) {
          # Close all workers
          for my $s (keys(%$workers)) {
            close($workers->{ $s }->{socket});
          }
          close($self->{app_socket});
          last;
        }
      }
      
      # Set up selectors:
      # Always check app for read, unless shutting down.  App write only if
      # there's something in @result_queue.
      my (@rpids, @wpids);
      my $rs = IO::Select->new();
      if ( ! $self->{shutting_down}) {
        $rs->add($self->{app_socket});
        push(@rpids, "app");
      }
      my $ws = IO::Select->new();
      if ( @{ $self->{result_queue} } ) {
        $ws->add($self->{app_socket});
        push(@wpids, "app");
      }
      
      # Check workers for read only if not idle
      # Otherwise, IF job_queue isn't empty,
      # check nonidle workers for write.
      for my $s (keys(%$workers)) {
        if ( $workers->{ $s }->{idle}) {
          if ( @{ $self->{job_queue} }  ||  exists($workers->{ $s }->{partial_job})) {
            $ws->add($workers->{ $s }->{socket});
            push(@wpids, $workers->{ $s }->{pid});
          }
        } else {
          $rs->add($workers->{ $s }->{socket});
          push(@rpids, $workers->{ $s }->{pid});
        }
      }
      
      # Blocking
      my @rdy = IO::Select->select($rs, $ws, undef);
      if ( ! @rdy) {
        if ($! == EINTR) {
          # signal interrupt, continue waiting
          next;
        }
        croak("select failed: $!");
      }
      my ($r, $w) = @rdy[0,1];
      
      # Now we have zero or more reabable sockets, and
      # zero or more writable sockets, but there's at
      # least one socket among the two groups.
      # Read first, as things read can be further handled
      # by writables immediately afterwards.
      
      for my $rh (@$r) {
        my ($source, $queue, $rstream);
        if ($rh != $self->{app_socket}) {
          $source = $workers->{$rh}->{pid};
          $queue = $self->{result_queue};
          $rstream = \$workers->{$rh}->{result_stream};
        } else {
          $source = 'app';
          $queue = $self->{job_queue};
          $rstream = \$self->{job_stream};
        }
        
        $self->read($rh, $queue, $rstream, 'boss');
      }

      for my $wh (@$w) {
        my $source = exists($workers->{ $wh }) ? $workers->{ $wh }->{pid} : "app";
        $self->write($wh);
      }
    }
  };
  if ($@) {
    croak($@);
  }
}

# -----------------------------------------------------------------
sub write {
  my ($self, $socket)=@_;
  if ($socket == $self->{app_socket}) {
    $self->write_app($socket);
  } else {
    $self->write_worker($socket);
  }
}

# -----------------------------------------------------------------
sub write_app {
  my ($self, $socket)=@_;
  
  # App socket: write all bytes until would block, or complete.
  # This means process result_queue in order, doing as many elems
  # as possible.  Don't remove from the queue until complete.  In
  # other words, the first item on the queue may be a partial from
  # the previous write attempt.
  my $queue = $self->{result_queue};
  while (@$queue) {
    while ( $queue->[0] ) {
      my $n = syswrite($socket, $queue->[0]);
      if ( ! defined($n)) {
        # Block or real socket error
        if ($! == EWOULDBLOCK) {
          # That's it for this socket, try another, or select again.
          return;
        } else {
          croak("sysread: $!");
        }
      }
        
      elsif ($n == 0) {
        # Application error: socket has been closed prematurely by other party.
        # Boss is supposed to close app socket before app.  App tells Boss to
        # stop, but it only happens after all existing work is completed, and
        # data is sent back to app.
        croak("app socket closed prematurely");
          
      } else {
        # wrote some bytes, remove them from the queue elem
        substr($queue->[0], 0, $n) = '';
      }
    }
    # queue elem is empty, remove it, go try next one
    shift(@$queue);
  }
  # queue is empty, all written!
}
 
# -----------------------------------------------------------------
sub write_worker {
  my ($self, $socket)=@_;
   
  # A worker: check to see if we have a remaining partial
  # job we already started to send.  If so, continue with this.
  # Otherwise, take a *single* job off the job_queue, and send that.
  # When we've gotten either complete, or would block, write remaining
  # portion to per-worker job-in-progress, or make it '' if complete.
  # With worker, we only send ONE job, never more.
  # Once job send is complete, mark worker not-idle.
  
  if ( ! exists($self->{workers}->{ $socket }->{partial_job})) {
    if (@{ $self->{job_queue} }) {
      $self->{workers}->{ $socket }->{partial_job} = shift(@{ $self->{job_queue} });
    } else {
      # Nothing left on queue.  Remember, we select on *all* idle workers,
      # even if there's only one job on the queue.
      return;
    }
  }
  my $rjob = \$self->{workers}->{ $socket }->{partial_job};
  
  while ( length($$rjob) ) {
    my $n = syswrite($socket, $$rjob);
    if ( ! defined($n)) {
      # Block or real socket error
      if ($! == EWOULDBLOCK) {
        # That's it for this socket, try another, or select again.
        return;
      } else {
        croak("sysread: $!");
      }
    }
        
    elsif ($n == 0) {
      # Application error: socket has been closed prematurely by other party.
      # Boss is supposed to close worker socket before worker - that's how
      # worker knows to exit.
      croak("worker socket closed prematurely (pid " . $self->{workers}->{ $socket }->{pid} . ")");
          
    } else {
      # wrote some bytes, remove them from the job
      substr($$rjob, 0, $n) = '';
    }
  }
  # job all written!
  delete($self->{workers}->{ $socket }->{partial_job});
  $self->{workers}->{ $socket }->{idle} = 0;
}

# -----------------------------------------------------------------
# Boss exits loop on error, wouldblock, or shutdown msg (socket close).
# Worker exits loop on error, recd full record, or boss socket close.
# App exits loop on error, recd full record, wouldblock (nb only), early boss close (error).
# Stream (as external ref) isn't needed for worker, as it's blocking, and only reads a single
# record, no more.
# So $rstream can be undef, and if so, we init locally.
sub read {
  my ($self, $socket, $queue, $rstream, $iam)=@_;
  my $stream;
  $rstream = \$stream if ! defined($rstream);
  $$rstream = '' if ! defined($$rstream);
  while ( 1 ) {
    my $n = sysread($socket, $$rstream, 1024, length($$rstream));
    if ( ! defined($n)) {
      if ($! == EINTR) {
        # signal interrupt, continue reading
        next;
      } elsif ($! == EWOULDBLOCK) {
        last;    # No bytes recd, no need to chunk.
      } else {
        croak("sysread: $!");
      }
          
    } elsif ($n == 0) {
      # Application error: socket has been closed prematurely by other party.
      # Boss is supposed to close worker socket before worker - that's how
      # worker knows to exit.
      # Boss is supposed to close app socket before app.  App tells Boss to
      # stop, but it only happens after all existing work is completed, and
      # data is sent back to app.
      if ($iam eq 'boss') {
        if ($socket == $self->{app_socket}) {
          $self->{shutting_down} = 1;
        } elsif (exists($self->{workers}->{$socket})) {
          croak("Boss: worker closed prematurely (pid " . $self->{workers}->{ $socket }->{pid} . ")");
        }
      } elsif ($iam eq 'worker') {
        close($socket);
      } else {    # i am app
        croak("App: boss closed prematurely (pid " . $self->{boss_pid} . ")");
      }

      # if we didn't croak...
      last;
      
    } else {
      # We actually read some bytes.  See if we can chunk
      # out any record(s).
      
      
      # Split on delimiter
      my @records = split(/(?<=$self->{msg_delimiter})/, $$rstream);

      # All but last elem are full records
      my $rcount=$#records;
      push(@$queue, @records[0..$#records-1]);

      # Deal with last elem, which may or may not be full record
      if ($records[ $#records ] =~ /$self->{msg_delimiter}$/) {
        # We have a full record
        $rcount++;
        push(@$queue, $records[ $#records ]);
        $$rstream = '';
        if (exists($self->{workers}->{ $socket })) {
          $self->{workers}->{ $socket }->{idle} = 1;
        }
      } else {
        $$rstream = $records[$#records];
      }

      # Boss grabs all it can get, only exiting loop on wouldblock.
      # App (even nb method), and workers all exit when one full
      # record is received.
      last if $rcount  &&  $iam ne 'boss';
    }
  }
}

# -----------------------------------------------------------------
# Worker process; single blocking socket open to boss.
# Blocking select loop:
# Only do read OR write, not both.  We never want more than a single
# job at a time.  So, if no job currently, read, waiting for one.
# Get a job, perform it, and try to write results.
# Send delimiter, which tells boss it has all the results, and we're ready
# for another job.
#
sub worker_loop {
  my ($self)=@_;
  eval {
    $SIG{CHLD} = 'IGNORE';

    $self->{init_handler}->()  if $self->{init_handler};
    
    # String buffers to store serialized data: in and out.
    my $result_stream;
    while ( 1 ) {
      if (defined($result_stream)) {
        # We have a result: write it to boss
        
        my $n = syswrite( $self->{socket}, $result_stream);
        croak("syswrite: $!") if ! defined($n);
        $result_stream = undef;
        # will return to top of loop
        
      } else {
        # Get job from boss
        
        my @queue;
        $self->read($self->{socket}, \@queue, undef, 'worker');
        return if ! @queue;
        
        my $job = $self->deserialize($queue[0]);
        my $result;
        eval {
          local $SIG{ALRM} = sub {
            die("Work handler timed out");
          };

          # Set alarm
          alarm($self->{global_timeout});

          # Invoke handler and get result
          $result = $self->{work_handler}->($job);

          # Disable alarm
          alarm(0);
        };

        # Warn on errors
        if ($@) {
          carp("Worker $$ error: $@");
        }
        
        $result_stream = $self->serialize($result);
      }
    }
  };
  if ($@) {
    croak($@);
  }
}

1;
__END__

=head1 NAME

Parallel::Fork::BossWorkerAsync - Perl extension for creating asynchronous forking queue processing applications.

=head1 SYNOPSIS

  use Parallel::Fork::BossWorkerAsync ();
  my $bw = Parallel::Fork::BossWorkerAsync->new(
    work_handler => \&work
  );

  # Jobs are hashrefs
  $bw->add_work( {key => 'value'} );
  while ($bw->pending()) {
    my $ref = $bw->get_result();
  }
  $bw->shut_down();

  sub work {
    my ($job)=@_;
    # do something with hash ref $job

    # Return values are hashrefs
    return { };
  }

=head1 DESCRIPTION

Parallel::Fork::BossWorkerAsync implements a multiprocess preforking server.  On construction, the current process forks a "Boss" process (the server), which then forks one or more "Worker" processes.  The Boss acts as a manager, accepting jobs from the main process, queueing and passing them to the next available idle Worker.  The Boss then listens for, and collects any responses from the Workers as they complete jobs, queueing them for the main process.  At any time, the main process can collect available responses from the Boss.

Having a separate Boss process allows the main application to behave asynchronously with respect to the queued work.  Also, since the Boss is acting as a server, the application can send it more work at any time.

=head1 METHODS

=head2 new(...)

Creates and returns a new Parallel::Fork::BossWorkerAsync object.

  my $bw = Parallel::Fork::BossWorkerAsync->new(
    work_handler    => \&work_sub,
    result_handler  => \&result_sub,
    init_handler    => \&init_sub,
    global_timeout  => 0,
    worker_count    => 3,
    msg_delimiter   => "\0\0\0"
  );

=over 4

=item * C<< work_handler => \&work_sub >>
  
work_handler is the only required argument.  The sub is called with it's first
and only argument being one of the values in the work queue. Each worker calls
this sub each time it receives work from the boss process. The handler may trap
$SIG{ALRM}, which may be called if global_timeout is specified.

The work_handler should clean up after itself, as the workers may call the
work_handler more than once.

=item * C<< result_handler => \&result_sub >>

The result_handler argument is optional, the sub is called with it's first
and only argument being the return value of work_handler. The boss process
calls this sub each time a worker returns data. This subroutine is not affected
by the value of global_timeout.

=item * C<< init_handler => \&init_sub >>

The init_handler argument is optional.  It accepts no arguments and has no
return value.  It is called only once by each worker as it loads and before
entering the job processing loop. This subroutine is not affected by the
value of global_timeout.  This could be used to connect to a database, etc.

=item * C<< global_timeout => $seconds >>

By default, a handler can execute forever. If global_timeout is specified, an
alarm is setup to terminate the work_handler so processing can continue.

=item * C<< worker_count => $count >>

By default, 3 workers are started to process the data queue. Specifying
worker_count can scale the worker count to any number of workers you wish.

=item * C<< msg_delimiter => $delimiter >>

Sending messages to and from the child processes is accomplished using
Data::Dumper. When transmitting data, a delimiter must be used to identify the
breaks in messages. By default, this delimiter is "\0\0\0".  This delimiter may
not appear in your data.

=head2 add_work(\%work)

Adds work to the instance's queue.  It accepts a list of hash refs.  add_work() can
be called at any time before shut_down().  All work can be added at the beginning,
and then the results gathered, or these can be interleaved: add a few jobs, grab the
results of one of them, add a few more, grab more results, etc.

Note: Jobs are not guaranteed to be processed in the order they're added.  This is
because they are farmed out to multiple workers running concurrently.

  $bw->add_work({data => "my data"}, {data => "more stuff"}, ...);

=head2 B<pending()>

This simple function returns a true value if there are jobs that have been submitted
for which the results have not yet been retrieved.

Note: This says nothing about the readiness of the results.  Just that at some point,
now or in the future, the results will be available for collection.

  while ($bw->pending()) { }

=head2 B<get_result()>

Requests the next single available job result from the Boss' result queue.  Returns the return value of the work_handler.  If there is a result_handler defined, it's called here, and the return value of this function is returned instead.  Depending on what your work_handler, or result_handler, does, it may not be interesting to capture this result.

By default, get_result() is a blocking call.  If there are no completed job results available, main application processing will stop here and wait.

  my $href = $bw->get_result();

If you want nonblocking behavior:

  my $href = $bw->get_result( blocking => 0 );
  -OR-
  my $href = $bw->get_result_nb();

In this case, if the call would block, because there is no result to retrieve, it returns immediately, returning undef.

=head2 B<shut_down()>

Tells the Boss and all Workers to exit.  All results should have been retrieved via get_result() prior to calling shut_down().  If shut_down() is called earlier, the queue *will* be processed, but depending on timing the subsequent calls to get_result() may fail if the boss has already written all result data into the socket buffer and exited.

  $bw->shut_down();

If you just want the Boss and Workers to go away, and don't care about work in progress, use:

  $bw->shut_down( force => 1 );

=head1 SEE ALSO

B<Parallel::Fork::BossWorkerAsync> received much inspiration from the fine L<Parallel::Fork::BossWorker>.  The main difference is that BossWorkerAsync introduces the Boss as a separate process, a bi-directional multiplexing, listening, queueing, server.  This allows the calling process much more freedom in interacting with, or ignoring, the Boss.

=head1 BUGS

Please report bugs to jvannucci@cpan.org.

Forked processes and threads don't mix well.  It's a good idea to construct Parallel::Fork::BossWorkerAsync before multiple threads are created.

I have no idea if this module will work on Windows.

=head1 CREDITS

Many thanks to Jeff Rodriguez for his module Parallel::Fork::BossWorker.

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2009 by joe vannucci, E<lt>jvannucci@cpan.orgE<gt>

All rights reserved.  This library is free software; you can redistribute
it and/or modify it under the same terms as Perl itself.

=cut
