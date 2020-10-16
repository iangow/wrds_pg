DO $$
BEGIN
IF NOT EXISTS (
    SELECT 1
    FROM pg_type a
    INNER JOIN pg_namespace b
    ON a.typnamespace=b.oid
    WHERE typname = 'parsed_name' AND nspname = 'risk') THEN

    CREATE TYPE risk.parsed_name AS
        (
        	prefix text,
        	first_name text,
        	middle_initial text,
        	last_name text,
        	suffix text);
END IF;
END$$;

ALTER TYPE risk.parsed_name
    OWNER TO risk;

CREATE OR REPLACE FUNCTION risk.extract_name(text)
  RETURNS risk.parsed_name AS
$BODY$
    my $first_name;
    my $last_name;
    my $name;
    my $prefix;
    my $last_name_suffix;
    my $suffix;
    my $middle_initial;

    if (defined($_[0])) {

        # Fix misspellings
        $temp = $_[0];
        $misspellings = 'irector|Dirctor|Director Director|Directror|Driector|Directo';
        $temp =~ s/\b($misspellings)\b/Director/;
        $temp =~ s/\bTurstee/Trustee/;

        # Fix other errors
        $temp	=~ s/\*//g;			# Remove asterisks
        $temp =~ s/\s{2,}/ /g;		# Remove any multiple spaces
        $temp =~ s/by (majority|plurality) vote$//i;	# Delete certain phrases
        $temp =~ s/ as .*Director//;
        $temp =~ s/ (as|by Holders of) (Class (A|B) )?(Common )?Stock//i;
        $temp =~ s/ \(Don't Advance\)//i;
        $temp =~ s/ \(DO NOT ADVANCE\)//i;
        $temp =~ s/Philip R, Roth/Philip R. Roth/;
        $temp =~ s/The Duke Of/Duke/i;
        $temp =~ s/Keith A, Meister/Keith A. Meister/;
        $temp =~ s/(The (Right|Rt\.)? )?Hon(ou?rable|\.) /Rt. Hon. /i;
        $temp =~ s/Elect Director Norman S. Edelcup Elect.*/Elect Director Norman S. Edelcup/i;
        $temp =~ s/Require Majority Vote to Elect Directors in an Uncontested Election//i;
        $temp =~ s/Elect Director ohn\s+/Elect Director John /;

        # Change alternative forms
        $temp =~  s/Elect\s+(.*)\sas Director/Elect Director \1/;

        # Look for forms like "Elect Ian D. Gow" (i.e., no words other than "elect" and the name
        if (($temp =~ /^Elect(?! Director)/) &&
          !($temp =~ /\b(Auditors|Trust|Director|Company|Members|Inc\.|of|as|to)\b/)) {
          $temp =~ s/Elect (.*)/Elect Director \1/;
        }

        # Pull out text after "Elect director";
        # if the word "and" appears, delete the observation
        # as there are multiple names.
        if(!($temp  =~ /\sand\s/i) && $temp =~ /(?:Elect\s+Directors?)\s+(.+)$/i) {
            $name = $1;

            # Remove leading spaces
            $name =~ s/^\s+//;

            if ($name =~ ",") {

                # If there's a comma, put the part after the first comma into a suffix
                if ($name =~ /(.+?)\s+([\w']*?)\s?,\s?(.*)\s?$/) {
                    ($first_name, $last_name, $suffix) = ($1, $2, $3);
                }

            # Some suffixes are not always separated by a comma, but we can be confident that
            # they're suffixes. Pull these out too.
            } elsif ($name =~
                    /^(.*)\s+(.*?)\s+(JR\s?\.?|PH\.?D\.?|II|III|IV|V|VI|M\.?D\.?|\(RET(\.|ired)?\)|3D|CBE)$/i) {
                    ($first_name, $last_name, $suffix) =  ($1, $2, $3);
            } else {
                $name =~ s/\s+$//;
                # If there's no suffix...
                if ($name =~ /^(.+)\s+(.+?)$/) {
                    ($first_name, $last_name) = ($1, $2);
                    $suffix="";
                }
            }

            # Pull out prefixes like Mr, Dr, etc.
            if ($first_name =~
                /^((?:Amb\.|Ambassador|(?:Rear|Vice )?(?:Adm\.|Admiral)|RADM|(?:Maj\. |Major )?Gen\.)\.? )?(.*)$/i) {
                    ($prefix, $first_name) = ($1, $2);
            }
            if ($prefix eq "" & $first_name =~ /^((?:(?:Lieutenant |Major )?General)\.? )?(.*)$/i) {
                ($prefix, $first_name) = ($1, $2);
            }
            if ($prefix eq "" & $first_name =~
                /^((?:Lt Gen|Hon\.|Prof\.|Professor|Rev\.|Rt\. Hon\.?|Sir|Dr|Mr|Mrs|Ms)\.? )?(.*)$/i) {
                    ($prefix, $first_name) = ($1, $2);
            }
            if ($prefix eq "" & $first_name =~ /^(Sen\. |Senator )?(.*)$/i) {
                ($prefix, $first_name) = ($1, $2);
            }
        }

    }

    # Remove last-name prefixes from first names
    if ($first_name =~ /(.+?)((?:\s[a-z]+)+)$/) {
        $first_name = $1;
        $last_name_prefix = $2;
        $last_name_prefix =~ s/^\s+//;
        $last_name = $last_name_prefix . ' ' . $last_name;
    }

    if ($first_name =~ /(.*?)\s+(.*)$/) {
        $first_name = $1;
        $middle_initial = $2;
    }

    return {first_name => $first_name, middle_initial => $middle_initial,
            last_name => $last_name, suffix => $suffix, prefix => $prefix };
$BODY$
  LANGUAGE plperl VOLATILE
  COST 100;

ALTER FUNCTION risk.extract_name(text) OWNER TO risk;
