use strict;
use warnings;
use Module::Build;

my $builder = Module::Build->new(
    module_name         => 'SQL::Abstract::More',
    license             => 'perl',
    dist_author         => q{Laurent Dami <laurent.dami@justice.ge.ch>},
    dist_version_from   => 'lib/SQL/Abstract/More.pm',
    requires => {
        'MRO::Compat'          => 0,
        'SQL::Abstract'        => 1.73,
        'Params::Validate'     => 0,
        'Scalar::Does'         => 0,
        'parent'               => 0,
        'namespace::clean'     => 0,
     },
    build_requires => {
        'Test::More'          => 0,
        'Test::Exception'     => 0,
        'SQL::Abstract::Test' => 0,
    },

    add_to_cleanup      => [ 'SQL-Abstract-More-*' ],
    create_makefile_pl => 'traditional',
    meta_merge => {
      resources => {
        repository => 'https://github.com/damil/SQL-Abstract-More',
       }
     },
);

$builder->create_build_script();
