
use DBI;
use Net::LDAP;
use strict;


my $db = DBI->connect("DBI:SQLite:dbname=testdb","","");
my $ldap = Net::LDAP->new("127.0.0.1");

my $result = $ldap->bind(dn=>"cn=Manager,dc=g0n,dc=net",
			password=>"XXXXX");
if ($result->code){die $result->error}


use Data::Sync;
my $logfile;
open ($logfile,">","logfile.txt");
my $synchandle = Data::Sync->new(log=>"STDOUT");

$synchandle->source($db,{select=>"select name,postal,telephone from target"});

			
$synchandle->target($ldap);
			

$synchandle->mappings(NAME=>'cn',POSTAL=>'postalAddress','TELEPHONE'=>'telephoneNumber');

$synchandle->buildattributes(dn=>"cn=%NAME%,ou=testcontainer,dc=g0n,dc=net",
				sn=>"%NAME%",
			objectclass=>"organizationalPerson");

$synchandle->transforms(telephoneNumber=>'s/^(\d) (\d\d\d)/$2 $1/ ');
print  $synchandle->run;

