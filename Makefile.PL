use 5.006;
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
if ($^O =~ m!mswin!i) {
  die "No support for OS\n";
}

WriteMakefile(
    NAME              => 'Parallel::Fork::BossWorkerAsync',
    VERSION_FROM      => 'lib/Parallel/Fork/BossWorkerAsync.pm', # finds $VERSION
    PREREQ_PM         => {}, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM  => 'lib/Parallel/Fork/BossWorkerAsync.pm', # retrieve abstract from module
       AUTHOR         => 'joe vannucci <joe.vannucci@gmail.com>') : ()),
);
