use strict;
use warnings;
use Test;
use Data::Sync;

BEGIN
{
	plan tests=>7
}

if (-e "config.dds"){unlink("config.dds")}
my $synchandle = Data::Sync->new();

# test internal transformation methods of Data::Sync

my @AoH = (	{"name"=>"Test User",
		"address"=>"1 Test Street",
		"phone"=>"01234 567890",
		"description"=>"Test string"},
		{"name"=>"Test user 2",
		"address"=>["1 Office Street","2 Office Street"],
		"phone"=>["01234 567891","01234 567892"]},
		{"name"=>"Test user 3",
		"address"=>[["1 Home Street","2 Home Street"],"3 Office Street"]},
		{"name"=>"Test user 4",
		"longaddress"=>"123 Test Street\nTest Town\nUK"});


$synchandle->transforms(name=>"stripspaces",
			address=>sub{
					my $var=shift;
					$var=~s/Street/St./g;
					return $var},
			phone=>'s/^0(\d{4})/\+44 $1/',
			longaddress=>"stripnewlines",
			"description"=>"");

$synchandle->save("config.dds");
my $sync2 = Data::Sync->new(configfile=>"config.dds");


my $result = $sync2->runTransform(\@AoH);

		
# check names
my $namesok=1;
if ($result->[0]->{'name'} ne "TestUser"){$namesok--}
if ($result->[1]->{'name'} ne "TestUser2"){$namesok--}
if ($result->[2]->{'name'} ne "TestUser3"){$namesok--}
ok($namesok);

# check addresses
my $addressesok=1;
if ($result->[0]->{'address'} ne "1 Test St."){$addressesok--}
if ($result->[1]->{'address'}->[0] ne "1 Office St."){$namesok--}
if ($result->[1]->{'address'}->[1] ne "2 Office St."){$namesok--}
if ($result->[2]->{'address'}->[1] ne "3 Office St."){$namesok--}
if ($result->[2]->{'address'}->[0]->[0] ne "1 Home St."){$namesok--}
if ($result->[2]->{'address'}->[0]->[1] ne "2 Home St."){$namesok--}
ok($namesok);

# check phone numbers
my $phoneok=1;
if ($result->[0]->{'phone'} ne "+44 1234 567890"){$phoneok--}
if ($result->[1]->{'phone'}->[0] ne "+44 1234 567891"){$phoneok--}
if ($result->[1]->{'phone'}->[1] ne "+44 1234 567892"){$phoneok--}
if ($result->[2]->{'phone'}){$phoneok--}
ok($phoneok);

# check for newline removal
my $nlok=1;
if ($result->[3]->{'longaddress'} ne "123 Test Street Test Town UK"){$nlok--}
ok($nlok);

#check for null transform
my $nullok=1;
if ($result->[0]->{'description'} ne "Test string"){$nullok--}
ok($nullok);

# check for loading non-existent file
my $nonok=1;
if ($synchandle->load("fakefile.dds")){$nonok--};
if ($synchandle->error ne "Unable to open fakefile.dds for reading because No such file or directory"){$nonok--}
ok($nonok);

# check for loading dud file
my $dudok=1;
my $dudfile;
open ($dudfile,">","dudfile.dds") or die "Can't open dummy file for writing because $!";
print $dudfile "#DUMMY\n";
close $dudfile;
if ($synchandle->load("dudfile.dds")){$dudok--}
if ($synchandle->error ne "Unsuccessful load from dudfile.dds"){$dudok--}
ok($dudok);
