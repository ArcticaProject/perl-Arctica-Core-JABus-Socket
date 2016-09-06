use strict;
use ExtUtils::MakeMaker;

WriteMakefile (
  AUTHOR        => 'Arctica Project Developers',
  ABSTRACT      => "Arctica Framework Core Modules",
  NAME          => 'Arctica::Core::JABus::Socket',
  (eval { ExtUtils::MakeMaker->VERSION(6.21) } ? (LICENSE => 'perl') : ()),
  PREREQ_PM     => {
    'Test::More' => '0.47',
  },
  VERSION_FROM  => 'lib/Arctica/Core/JABus/Socket/Version.pm',
  clean               => { FILES => '' },
);
