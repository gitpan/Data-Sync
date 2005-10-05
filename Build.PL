use Module::Build;
my $build = Module::Build->new
(
	module_name => 'Data::Sync',
	license => 'perl',
	requires =>
		{
			'Data::Dump::Streamer' => '1.14'
		}
);
$build->create_build_script;
