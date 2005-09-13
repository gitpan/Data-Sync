#####################################################################
# Data::Sync
#
# Classes to make development of metadirectory/datapump apps
# simple and fast
#
# C Colbourn 2005
#
#####################################################################
# Revision
# ========
#
# 0.01	CColbourn		New module
#
# 0.02	CColbourn		Enhancements - see POD CHANGES
#
#####################################################################
# Notes
# =====
#
#####################################################################

use strict;
use warnings;
use Data::Dump::Streamer qw(:undump);

package Data::Sync;
use vars qw($VERSION);
$VERSION="0.02";

#####################################################################
# New - constructor of datasync object
#
# takes parameters. 
# returns blessed object
#
#####################################################################
sub new
{
	my $self = shift;
	my %synchash;
	my %params = @_;

	# make the object first!
	my $syncobject = bless \%synchash,$self;

	# define logging. If logging not set, use a coderef of return
	if ($params{'log'})
	{
		$syncobject->{'log'} = \&log;
		$syncobject->{'loghandle'} = $params{'log'};
	}
	else
	{
		$syncobject->{'log'} = sub{return}
	}
	if ($params{'configfile'})
	{
		my $return = $syncobject->load($params{'configfile'});
		if (!$return)
		{
			$self->{'log'}->($self->{'loghandle'},"ERROR: Not a readable config file ".$params{'configfile'});
			$self->{'lasterror'} = "Not a readable config file ".$params{'configfile'};
			return
		}
	}
	# return the object
	return $syncobject; 
}

#####################################################################
# source
#
# defines the source data type & match critera
#
# takes $handle,\%search criteria
# returns true on successful definition
#####################################################################
sub source
{
	my $self = shift;
	my $handle = shift;
	my $criteriaref = shift;

	# do this regardless
	$self->{'readhandle'}=$handle;

	if (!$criteriaref)
	{
		# this /should/ mean the config is coming from a file
		# so return if the configs already been loaded (and
		# if not, it won't hurt anything to continue)
		if ($self->{'readcriteria'})
		{
			return 1;
		}
	}
	$self->{'readcriteria'} = $criteriaref;

	# Create coderef for LDAP
	if ($handle =~/LDAP/)
	{
		$self->{'read'} = 
			sub
			{
				my $self = shift;
				my $result = $self->{'readhandle'}->search
					(filter=>$self->{'readcriteria'}->{'filter'},
					base=>$self->{'readcriteria'}->{'base'},
					scope=>$self->{'readcriteria'}->{'scope'},
					attrs=>$self->{'readcriteria'}->{'attrs'});
				if ($result->code)
				{
					$self->{'log'}->($self->{'loghandle'},"ERROR:".$result->error);
					return $result->error
				}
				else {return $result}
			}
	}

	# everything else will be DBI/SQL
	else
	{
		$self->{'read'} =
			sub
			{
				my $self = shift;

				my $stm = $self->{'readhandle'}->prepare($self->{'readcriteria'}->{'select'}) or return;
				my $result = $stm->execute;
				if ($result eq "0E0"){return $stm}
				else
				{
					$self->{'log'}->($self->{'loghandle'},"ERROR: Could not read from database");
					$self->{'lasterror'}="ERROR: Could not read from database";
					return undef;
				}
			}
	}

	1;
}

#########################################################
# target - define the data target
#
# takes $handle, \%writecriteria
#
# returns 1 on success
#
#########################################################
sub target
{
	my $self = shift;
	my $handle = shift;

	$self->{'writehandle'} = $handle;
	
	my $criteriaref = shift;
	if (!$criteriaref)
	{
		# this /may/ mean the config is coming from a file
		# so return if the configs already been loaded (and
		# if not, it may be an ldap target so continue;
		if ($self->{'readcriteria'} && $handle!~/LDAP/)
		{
			return 1;
		}
	}
	else 
	{
		$self->{'writecriteria'} = $criteriaref;
	}

	
	# create coderef to write to LDAP
	if ($handle =~/LDAP/)
	{
		$self->{'write'} =
			sub
			{
				my $self = shift;
				my $writedata = shift;
				foreach my $line (@$writedata)
				{
					my $dn = $line->{'dn'};
					delete $line->{'dn'};
					$self->{'log'}->($self->{'loghandle'},"Modifying $dn, values ".join ",",values %$line);
					my $result =
						$self->{'writehandle'}->modify
							(
								dn=>$dn,
								replace=>[%$line]
							);
					if ($result->code)
					{
					$self->{'log'}->($self->{'loghandle'},"Modify failed, adding $dn, values ".join ",",values %$line);
						$result =
							$self->{'writehandle'}->add
								(
									dn=>$dn,
									attrs=>[%$line]
								)
					}
					if ($result->code)
					{
						$self->{'log'}->($self->{'loghandle'},"ERROR: ".$result->error);
						$self->{'lasterror'}="ERROR: Add failed :".$result->error;
						return undef;
					}
				}
		}
	}

	# write coderef for DBI
	if ($handle =~/DBI/)
	{
		$self->{'write'} =
			sub
			{
				my $self = shift;
				my $writedata = shift;

				for my $line (@$writedata)
				{
					my $update = "update ".$self->{'writecriteria'}->{'table'}. " set ";
						

					# DANGER! this initially used keys %$line, but order can't be guaranteed.

					my (@keys,@values);

					for (keys %$line)
					{
						push @keys,$_;
						push @values,$$line{$_}
					}

					$update.=join "=?,",@keys;

					$update .="=? where ";
					$update .= $self->{'writecriteria'}->{'index'};
					$update .="=?";
						$self->{'log'}->($self->{'loghandle'},"Updating $update, ".join ",",@values);

					my $stm = $self->{'writehandle'}->prepare($update);
					my $result = $stm->execute(@values,$line->{$self->{'writecriteria'}->{'index'}});
					if ($result eq "0E0")
					{


						my $insert = "insert into ".$self->{'writecriteria'}->{'table'}." (";
						$insert .= join ",",@keys;
						$insert .=") VALUES (";
						$insert .=join ",",map { "?" } (0..scalar @values-1);
						$insert .=")";

						$self->{'log'}->($self->{'loghandle'},"Update failed, adding $insert, ".join ",",@values);
						$stm = $self->{'writehandle'}->prepare($insert);
						$result = $stm->execute(@values);
					}
					if ($result eq "0E0")
					{
						$self->{'log'}->($self->{'loghandle'},"ERROR: Add failed because ".$self->{'writehandle'}->errstr);
						$self->{'lasterror'}="ERROR: Add failed because ".$self->{'writehandle'}->errstr;
					}
					
				}

			}
		}
	1;
}

########################################################
# sourceToAoH
#
# Convert data from source to an array of hashes
# so that there's a standard form to write data out
#
# takes data handle (LDAP result or DBI)
#
# returns ref to AoH
#
########################################################
sub sourceToAoH
{
	my $self = shift;
	my $handle = shift;

	my @records;

	# Convert LDAP
	if ($handle=~/LDAP/)
	{
		for my $entry ($handle->entries)
		{
			my %record;
			for my $attrib ($entry->attributes)
			{
				$record{$attrib} = $entry->get_value($attrib);
			}
			$self->{'log'}->($self->{'loghandle'},"Read ".$entry->dn." from the directory");
			push @records,\%record
		}
	}

	my $counter=0;
	if ($handle=~/DBI/)
	{
		while (my $entry = $handle->fetchrow_hashref)
		{
			my %record;
			for my $attrib (keys %$entry)
			{
				$record{$attrib} = $entry->{$attrib}
			}
			$self->{'log'}->($self->{'loghandle'},"Read entry ".++$counter." from the database");
			push @records,\%record;
		}
	}

	return \@records;
}

#############################################################
# Run - read the data, transform it, then write it.
#
# takes no parameters
# returns success or fail.
#
#############################################################
sub run
{
	my $self = shift;

	# fetch from source
	my $receivedata = $self->{'read'}->($self);

	# If we don't get anything back, return 0
	if (!$receivedata){return}

	# convert to an AoH
	my $AoHdata = $self->sourceToAoH($receivedata);

	# construct templated attributes
	$AoHdata = $self->makebuiltattributes($AoHdata);

	# remap attrib names to target names
	$AoHdata = $self->remap($AoHdata);

	# perform data transforms
	$AoHdata = $self->runTransform($AoHdata);

	# write to target
	my $result = $self->{'write'}->($self,$AoHdata);

	return $result;
}

#############################################################
# mappings - define mappings from source to target
#
# takes hash of sourceattrib=>targetattrib
#
# returns success or fail
#
#############################################################
sub mappings
{
	my $self = shift;
	my %params = @_;

	$self->{'map'} = \%params;

	return 1;
}

##############################################################
# remap - rename source keys in data to target keys
#
# takes data structure in AoH form
# returns data structure in AoH form
#
##############################################################
sub remap
{
	my $self = shift;
	my $data = shift;
	my @newdata;

	for my $line (@$data)
	{
		my %record;
		for my $attrib (keys %$line)
		{
			# retain unchanged name if nothing in map

			if ($self->{'map'}->{$attrib})
			{
				$record{$self->{'map'}->{$attrib}} = $$line{$attrib};
				$self->{'log'}->($self->{'loghandle'},"Remapped ".$attrib." to ".$self->{'map'}->{$attrib});
			}
			else
			{
				$record{$attrib}=$$line{$attrib}
			}
		}

		push @newdata,\%record;
	}

	return \@newdata;
}	


##############################################################
# transforms - define transformations of data
#
# takes hash of params
# returns success or fail
#
##############################################################
sub transforms
{
	my $self = shift;
	
	my %params=@_;

	# params are attrib=>regexstring or attrib=>coderef

	# if param is a regex, transform to a coderef
	for (keys %params)
	{
		# turn a function name into a coderef
		if ($params{$_} =~/^\w+$/)
		{
			$params{$_} = \&{$params{$_}};
		}
		elsif ($params{$_} !~/CODE/)
		{
			$params{$_}=eval "sub {my \$data=shift;
						\$data =~".$params{$_}.";
						return \$data;}";
		}
	}


	$self->{'transformations'}=\%params;

	return 1;

	
}

##############################################################
# runTransform - perform regexes and data transforms on
# the data
#
# takes AoH
# returns AoH
#
##############################################################
sub runTransform
{

	my $self = shift;
	my $inData = shift;
	my @outData;

	for my $line (@$inData)
	{
		my %record;
		for my $attrib (keys %$line)
		{
			# only convert if there is a transform for this
			if ($self->{'transformations'}->{$attrib})
			{
				my $before = $$line{$attrib};
				# handle possible multi valued attribs
				$record{$attrib} = $self->recursiveTransform($$line{$attrib},$self->{'transformations'}->{$attrib});
			}
			else
			{
				$record{$attrib} = $$line{$attrib}
			}
		}
		push @outData,\%record;
	}

	return \@outData;

}

############################################################
# recursiveTransform - recursively transform data
#
# takes attrib,$transformation
#
# returns transformed attrib
#
############################################################
sub recursiveTransform
{
	my $self = shift;
	my $data = shift;
	my $transformation = shift;

	if ($data =~/ARRAY/)
        {
                for (0..scalar @$data -1)
                {
                        $$data[$_] = $self->recursiveTransform($$data[$_],$transformation);
                }
        }

        elsif ($data =~/HASH/)
        {
                for my $inst (keys %$data)
                {
                        $$data{$inst} = $self->recursiveTransform($$data{$inst},$transformation);
                }
        }

        elsif ($transformation =~/CODE/)
        {
		my $before = $data;
		$data = $transformation->($data);
		$self->{'log'}->($self->{'loghandle'},"Transformed $before to ".$data);
	}

	return $data;
}
		
##########################################################
# buildattributes - fake up attributes from source data
#
# takes attribname=>'<template>' where %NAME% is the source
# data
# Note: this runs before runTransform, so data can be 
# templated here, then transformed in runTransform for more
# sophisticated constructions
#
# returns success\fail
#
###########################################################
sub buildattributes
{
	my $self = shift;
	my %attribs = @_;

	$self->{'buildattribs'} = \%attribs;

	return 1;
}	

#############################################################
# makebuiltattributes - add built attributes to the data map
#
# takes AoH
# returns AoH
#
#############################################################
sub makebuiltattributes
{
	my $self = shift;

	my $indataref = shift;
	my @inData = @$indataref;

	for my $line (@inData)
	{
		for my $newattrib (keys %{$self->{'buildattribs'}})
		{
			$$line{$newattrib} = $self->{'buildattribs'}->{$newattrib};
			# s/// the template
			$$line{$newattrib} =~s/%(.*?)%/$$line{$1}/g;
			$self->{'log'}->($self->{'loghandle'},"Created attribute $newattrib containing ".$$line{$newattrib});
		}
	}

	return \@inData;
}	

######################################################
# log - write logging information
#
# takes $fh,$string
#
# returns undef
#
######################################################
sub log
{
	my $fh = shift;
	my $string = shift;
	if ($fh eq "STDOUT")
	{
		print $string."\n";
	}
	else
	{
		print $fh $string."\n";
	}

	return;
}



########################################################################
# save - saves config to a DDS file
#
# takes filename
# returns success or fail
#
########################################################################
sub save
{
	my $self = shift;
	my $filename = shift;
	if (!$filename)
	{
		$self->{'log'}->($self->{'loghandle'},"ERROR: No filename supplied to save");
		$self->{'lasterror'}="ERROR: No filename supplied to save";
	}

	my $fh;
	open ($fh,">",$filename) or do
				{
					$self->{'lasterror'} = "Unable to open $filename for writing because $!";
					return;
				};


	my $dds = Data::Dump::Streamer->new;

	# clone the object and remove non serialisable or unwanted keys
	my $clone = \%$self;
	delete $clone->{'writehandle'};
	delete $clone->{'readhandle'};
	delete $clone->{'loghandle'};
	delete $clone->{'log'};

	print $fh $dds->Dump($clone)->Out();

	close $fh;

	return 1;
}

#######################################################################
# load - read back a config file
#
# takes filename
# returns 1 on success, 0 on fail
#
#######################################################################
sub load
{
	my $self = shift;
	my $filename = shift;

	if (!$filename)
	{
		$self->{'log'}->($self->{'loghandle'},"ERROR: No filename supplied to save");
		$self->{'lasterror'}="ERROR: No filename supplied to save";
	}
	
	my $Data_Sync1; # this is what Data::Dump::Streamer calls the object
	my $fh;
	open ($fh,"<",$filename) or do
				{
					$self->{'lasterror'} = "Unable to open $filename for reading because $!";
					return;
				};
	my $evalstring;
	while (<$fh>)
	{
		$evalstring .=$_;
	}

	eval $evalstring;
	my $successfulload;
	for my $attrib (keys %$Data_Sync1)
	{
		$self->{$attrib} = $Data_Sync1->{$attrib};
		$successfulload++;
	}

	if (!$successfulload)
	{
		if ($self->{'log'})
		{
			$self->{'log'}->($self->{'loghandle'},"ERROR: Unsuccessful load from $filename") ;
		}
		$self->{'lasterror'}="Unsuccessful load from $filename";
		return
	}

	return 1;

}

########################################################################
# error - returns last error
#
# takes no parameter
# returns error
#
########################################################################
sub error
{
	my $self = shift;

	return $self->{'lasterror'}
}


########################################################################
########################################################################
# transformation functions
########################################################################
########################################################################

sub stripspaces
{
	my $var = shift;
	$var=~s/ //g;
	return $var;
}

sub stripnewlines
{
	my $var = shift;
	$var=~s/\n/ /g;
	# (just in case)
	$var=~s/\r//g;
	return $var;
}






1;

#########################################################################
#########################################################################
# Nothing but POD from here on out
#########################################################################
#########################################################################

=pod

=head1 NAME

Data::Sync - A simple metadirectory/datapump module

=head1 SYNOPSIS

 use Data::Sync;

 my $sync = Data::Sync->new(log=>"STDOUT",[configfile=>"config.dds"]);

 $sync->source($dbhandle,{select=>"select * from testtable"});

 or

 $sync->source($ldaphandle,{filter=>"(cn=*)",
				scope=>"sub",
				base=>"ou=testcontainer,dc=test,dc=org"});

 $sync->target($dbhandle,{table=>'targettable',
				index=>'NAME'});

 or

 $sync->target($ldaphandle);

 $sync->mappings(FIRSTNAME=>'givenName',SURNAME=>'sn');

 $sync->buildattributes(dn=>"cn=%NAME%,ou=testcontainer,dc=test,dc=org",
			objectclass=>"organizationalUnit");

 $sync->transforms(PHONE=>'s/0(\d{4})/\+44 \($1\) /',
			ADDRESS=>sub{my $address=shift;
					$address=~s/\n/\<BR\>/g;
					return $address});

 $sync->save("filename");

 $sync->load("filename");

 $sync->run();

=head1 DESCRIPTION

Data::Sync is a simple metadirectory/data pump module. It automates a number of the common tasks required when syncing databases & ldap directories.

In order to use Data::Sync, you must define a source and a target. The first parameter to the source & target methods is a bound DBI/Net::LDAP handle.

Having defined your datasources, define how attributes map between them with mappings. If an attribute returned from the data source has no entry in the mapping table, it will be assumed that the name is the same in both databases.

Attributes can be built up from multiple other attributes using buildattributes. This uses a simple template, %FIELDNAME% which is replaced at run time with the value of the field from the current record. More complex modifications to data can be made with transforms, which runs after the built attributes are created.

Transforms can be made with the method transforms, which takes a hash of FIELDNAME=>transformation. This transformation can be one of three things: a regular expression in string form (see synopsis), the name of a predefined transformation supplied in Data::Sync, or a code ref.

Finally, if you are confident your data is all in the right format, use run. That will read the data from the source, modify it as you have specified, and write it to the target.

B<WARNING!> There is no implied or real warranty associated with the use of this software. That's fairly obvious, but worth repeating here. Metadirectory applications have the potential to destroy data in a very big way - they must be constructed carefully, and thoroughly tested before use on a live system.

=head1 CONSTRUCTOR

 my $sync = Data::Sync->new(log=>"STDOUT");
 my $sync = Data::Sync->new(log=>$fh);
 my $sync = Data::Sync->new(configfile=>"config.dds");

The constructor returns a Data::Sync object. To use logging, pass the string STDOUT as the log parameter to print logging to STDOUT, or a lexical filehandle.
Optionally, you can specify a config file, although you'll still need pass the db/ldap handles (only) to source & target.

=head1 METHODS

=head2 source

 $sync->source($dbhandle,{select=>"select * from testtable"});

 or

 $sync->source($ldaphandle,{filter=>"(cn=*)",
				scope=>"sub",
				base=>"ou=testcontainer,dc=test,dc=org"});

 or

 $sync->source($dbhandle); # only if loading config file

Requires a valid, bound (i.e. logged in) Net::LDAP or DBI handle, and a hash of parameters for the data source (assuming you aren't loading the config from a file). LDAP parameters are:
 filter
 scope
 base
 attrs
 controls

(See Net::LDAP for more details of these parameters).

DBI parameters are:
 select

=head2 target

 $sync->target($dbhandle,{table=>'targettable',
				index=>'NAME'});

 or

 $sync->target($ldaphandle);

 or

 $sync->target($db); # only if loading config from a file

Requires a valid, bound (i.e. logged in) DBI or Net::LDAP handle, and a hash of parameters (unless you are loading the config from a file). No parameters are required for LDAP data targets, but a dn must have been either read from the data source or constructed using buildattributes. Valid DBI parameters are

 table - the table you wish to write to on the data target
 index - the attribute you wish to use as an index

Note: if you are writing from DB to LDAP, you must construct all mandatory attributes using buildattributes, or additions will fail.

=head2 mappings

 $sync->mappings(FIRSTNAME=>'givenName',SURNAME=>'sn');

Maps individual field names from the data source, to their corresponding field names in the data target.

=head2 buildattributes

 $sync->buildattributes(dn=>"cn=%NAME%,ou=testcontainer,dc=test,dc=org",
			objectclass=>"organizationalUnit");

Builds new target attributes up from existing source attributes. A simple template form is used - the template should be a string variable, containing the source field name between % delimiters. If no % delimiters are found, the string will be written as a literal.

=head2 transforms

 $sync->transforms(	PHONE=>'s/0(\\d{4})/\+44 \(\$1\)'/’,
			OFFICE=>"stripspaces",
			ADDRESS=>sub{my $address=shift;
			$address=~s/\n/\<BR\>/g;
			return $address});

Converts each field in the source data using the parameters passed. Each parameter pair is the I<target> field name, along with a regex (in a string), a coderef, or a standard function. The following list of transformation functions are supplied in this version:

 stripspaces
 stripnewline

Note: If passing a regex in a string, make sure you use single quotes. Double quotes will invite perl to interpolate the contents, with unexpected results.

=head2 save

 $sync->save("filename");

Saves the config to a Data::Dump::Streamer file. Returns 1 on success.

=head2 load

 $sync->load("filename");

Loads the config from a Data::Dump::Streamer file previously created with save. You still need to define the source and target db/ldap handles with source & target, but if you've loaded the config from a file you can omit the hash of options.

=head2 run

 $sync->run() or die $sync->error."\n";

No parameters. Reads the data from the source, converts and renames it as defined in mappings, buildattributes and transforms, and writes it to the target.

=head2 error

 print $sync->error;

Returns the last error encountered by the module. This is set e.g. when a file fails to load correctly, when a sql error is encountered etc. When this occurs, the return value from the called function will be zero, and error() should be called to identify the problem.

=head1 PREREQS

Data::Dump::Streamer

If you are using LDAP datasources, you will need Net::LDAP.

If you are using DBI datasources, you will need DBI and the appropriate DBD drivers.

=head1 VERSION

0.02

=head1 TODO

Transform to convert multivalued attribs to single valued (choice: use value[index], join with delimiter)
		
Example using AnyData & XML

Deletion support (somehow, anyhow....)

Delta support?

Multiple sources in a single job?

Multiple targets in a single job?

UI to include non OO

Readprogress/writeprogress/transform progress coderefs to report progress (e.g. output a .)

=head1 CHANGES

Implemented load & save functions.

Implemented error function

Modified stripnewlines to replace with whitespace.

=head1 COPYRIGHT

Copyright (c) 2004-2005 Charles Colbourn. All rights reserved. This program is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

=head1 AUTHOR

Charles Colbourn
charlesc@g0n.net

=cut
