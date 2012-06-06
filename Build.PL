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
        'SQL::Abstract'        => 1.61,
        'Params::Validate'     => 0,
        'parent'               => 0,
        'namespace::clean'     => 0,
     },
    build_requires => {
        'Test::More'          => 0,
        'SQL::Abstract::Test' => 0,
    },

    add_to_cleanup      => [ 'SQL-Abstract-More-*' ],
    create_makefile_pl => 'traditional',
);

$builder->create_build_script();
