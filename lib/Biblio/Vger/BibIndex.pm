package Biblio::Vger::BibIndex;

use strict;
use warnings;

use MARC::Loop qw(TAG IND1 IND2 VALREF SUBS SUB_ID SUB_VALREF);
use Biblio::Vger;

# my $index = Biblio::Vger::BibIndex->new(
#     '020A' => 50,
#     'BBID' => { 'weight' => 100, 'override' => '951a' },
# );
# marcloop {
#     my ($leader, $fields) = @_;
#     my @norm  = $index->normalize;
#     my %match = $index->lookup->match(@norm);
#     ...
# }

my @all_index_codes = qw(
    008D 008L 008P 010A 010Z 019A 020A 020N 020R 020Z 022A 022Y 022Z 024A 024I
    024Y 024Z 027A 028A 028B 030A 030Z 0350 0359 035A 035Z 074A 074Z 100H 110H
    111H 1300 1301 1302 2100 2110 2120 2140 2220 2400 2401 2402 2450 2451 2452 2460 2470
    260D 4400 440A 600H 600T 610H 611H 630H 648H 6500 6501 6502 6503 6504 6505
    6506 6507 651H 654H 655H 6900 700H 700X 710H 710X 711H 711X 7300 7301 7302
    7400 7401 7402 7600 7620 7670 7720 7721 7730 7731 7800 7850 8000 800H 8100
    810H 8110 811H 8300 8301 8302 ISB3
);

sub new {
    my $cls = shift;
    @_ % 2 == 0 or die "Bad params";
    my $self = bless { }, $cls;
    $self->_compile(@_);
    return $self;
}

sub _compile {
    my $self = shift;
    my %matchpoint;
    while (@_) {
        my ($i, $p) = splice @_, 0, 2;
        my ($weight, $override, $exclude);
        if (ref $p) {
            ($weight, $override, $exclude) = @$p{qw(weight override exclude)};
        }
        else {
            $weight = $p;
        }
        my $indexer;
        if (defined $override) {
            $override =~ /^(...)(.)?$/ or die "Bad override: $override";
            my ($tag, $id) = ($1, $2);
            if (defined $id) {
                $indexer = sub { index_data_field_each_subfield(@_, $tag, $id) };
            }
            else {
                $indexer = sub { index_control_field(@_, $tag) };
            }
        }
        else {
            $indexer = $self->can("index_$i")
                || die "Unrecognized index: $i";
        }
        $exclude ||= sub { 0 };
        $matchpoint{$i} = { 'indexer' => $indexer, 'weight' => $weight, 'exclude' => $exclude };
    }
    $self->{'matchpoints'} = \%matchpoint;
}

sub index {
    my ($self, $fields) = @_;
    my %index;
    while (my ($i, $m) = each %{ $self->{'matchpoints'} }) {
        my $indexer = $m->{'indexer'};
        my @values = $indexer->($fields);
        $index{$i} = \@values if @values;
    }
    return \%index;
}

sub find {
    my $self = shift;
    my ($leader, $fields);
    if (@_ == 2 && ref($_[0]) eq '' && ref($_[1]) eq 'ARRAY') {
        ($leader, $fields) = @_;
    }
    elsif (@_ == 1 && ref($_[0]) eq 'SCALAR') {
        ($leader, $fields) = marcparse(@_);
    }
    else {
        die;
    }
    my $index = $self->index($fields);
    my $dbh = $self->{'dbh'} ||= Biblio::Vger->dbh or die "Can't connect";
    my $sth = $self->{'sth'} ||= $dbh->prepare('SELECT bib_id FROM bib_index WHERE index_code = ? AND normal_heading = ?')
        or die $dbh->errstr;
    my %match;
    my $matchpoints = $self->{'matchpoints'};
    while (my ($code, $values) = each %$index) {
        my $matchpoint = $matchpoints->{$code};
        foreach my $value (@$values) {
            $sth->execute($code, $value) or die $dbh->errstr;
            while (my ($bib_id) = $sth->fetchrow_array) {
                my $b = $match{$bib_id} ||= {};
                $b->{$code}{$value} = $matchpoint;
            }
        }
    }
    return \%match;
}

# --- Indexing functions

sub index_BBID { index_control_field(@_, '001') }

sub index_010A {
    index_data_field(@_, '010', 'a', sub {
        s{/.*}{};
        $_ = norm_value($_);
    });
}
sub index_010Z {
    index_data_field_each_subfield(@_, '010', 'z', sub {
        s{//.*}{};
        $_ = norm_value($_);
    });
}
sub index_019A { index_data_field_each_subfield(@_, '019', 'a') }

sub index_020A { index_data_field(@_, '020', 'a') }
sub index_020N {
    my @values = index_data_field(@_, '020', 'a');
    for (@values) {
        s/\s.*//;
        tr/- //d;
    }
    return @values;
}
sub index_020R { index_data_field(@_, '020', 'r') }
sub index_020Z { index_data_field(@_, '020', 'z') }

sub index_022A { index_data_field(@_, '022', 'a') }
sub index_022Y { index_data_field(@_, '022', 'y') }
sub index_022Z { index_data_field(@_, '022', 'z') }

sub index_024A { index_data_field(@_, '024', 'a') }
sub index_024I {
    my ($fields) = @_;
    # my @fields = grep { $_->[TAG] eq '024' && $_->[IND1] eq '3' && $_->[IND2] eq '2' } @$fields;
    my @fields = grep { $_->[TAG] eq '024' && $_->[IND1] eq '3' } @$fields;
    return index_data_field(\@fields, '024', 'a');
}
sub index_024Y {
    my ($fields) = @_;
    my @fields = grep { $_->[TAG] eq '024' && $_->[IND1] eq '3' && $_->[IND2] eq '2' } @$fields;
    return index_data_field(\@fields, '024', 'y');
}
sub index_024Z { index_data_field(@_, '024', 'z') }

sub index_027A { index_data_field(@_, '027', 'a') }

sub index_028A { index_data_field(@_, '028', 'a') }
sub index_028B { index_data_field(@_, '028', 'b') }

sub index_030A { index_data_field(@_, '030', 'a') }
sub index_030Z { index_data_field(@_, '030', 'z') }

sub index_0350 { index_data_field(@_, '035', 'a') }
sub index_0359 { index_data_field(@_, '035', '9') }
sub index_035A {
    index_data_field(@_, '035', 'a', sub {
        s{\(.+?\)}{};
        $_ = norm_control_value($_);
    });
}
sub index_035Z { index_data_field(@_, '035', 'z') }

sub index_074A { index_data_field(@_, '074', 'a') }
sub index_074Z { index_data_field(@_, '074', 'z') }

sub index_4400 { index_data_field(@_, '440', qw(a)) }
sub index_440A { index_data_field(@_, '440', qw(a)) }

sub index_648H { index_data_field(@_, '648', qw(a)) }

sub index_6500 { index_data_field_where_ind2(@_, '650', 0, qw(a b c v x y z)) }
sub index_6501 { index_data_field_where_ind2(@_, '650', 1, qw(a b c v x y z)) }
sub index_6502 { index_data_field_where_ind2(@_, '650', 2, qw(a b c v x y z)) }
sub index_6503 { index_data_field_where_ind2(@_, '650', 3, qw(a b c v x y z)) }
sub index_6504 { index_data_field_where_ind2(@_, '650', 4, qw(a b c v x y z)) }
sub index_6505 { index_data_field_where_ind2(@_, '650', 5, qw(a b c v x y z)) }
sub index_6506 { index_data_field_where_ind2(@_, '650', 6, qw(a b c v x y z)) }
sub index_6507 { index_data_field_where_ind2(@_, '650', 7, qw(a b c v x y z)) }

sub index_651H { index_data_field(@_, '651', qw(a v x)) }

sub index_654H { index_data_field(@_, '654', qw(a)) }

sub index_655H { index_data_field(@_, '655', qw(a)) }

sub index_6900 { index_data_field(@_, '690', qw(a)) }

sub index_700H { index_data_field(@_, '700', qw(a d t p q)) }
sub index_700X { index_data_field(@_, '700', qw(t)) }

sub index_710H { index_data_field(@_, '710', qw(a k n)) }
sub index_710X { index_data_field(@_, '710', qw(t)) }

sub index_711H { index_data_field(@_, '711', qw(a)) }
sub index_711X { index_data_field(@_, '711', qw(a)) }

sub index_7300 { index_data_field(@_, '730', qw(a)) }
sub index_7301 { index_data_field(@_, '730', qw(a)) }
sub index_7302 { index_data_field(@_, '730', qw(a)) }

sub index_7400 { index_data_field(@_, '740', qw(a)) }
sub index_7401 { index_data_field(@_, '740', qw(a)) }
sub index_7402 { index_data_field(@_, '740', qw(a)) }

sub index_7600 { index_data_field(@_, '760', qw(a)) }

sub index_7620 { index_data_field(@_, '762', qw(a)) }

sub index_7670 { index_data_field(@_, '767', qw(a)) }

sub index_7720 { index_data_field(@_, '772', qw(a)) }
sub index_7721 { index_data_field(@_, '772', qw(a)) }

sub index_7730 { index_data_field(@_, '773', qw(a)) }
sub index_7731 { index_data_field(@_, '773', qw(a)) }

sub index_7800 { index_data_field(@_, '780', qw(a)) }

sub index_7850 { index_data_field(@_, '785', qw(a)) }

sub index_8000 { index_data_field(@_, '800', qw(a)) }
sub index_800H { index_data_field(@_, '800', qw(a)) }

sub index_8100 { index_data_field(@_, '810', qw(a)) }
sub index_810H { index_data_field(@_, '810', qw(a)) }

sub index_8110 { index_data_field(@_, '811', qw(a)) }
sub index_811H { index_data_field(@_, '811', qw(a)) }

sub index_8300 { index_data_field(@_, '830', qw(a p v)) }
sub index_8301 { index_data_field(@_, '830', qw(a p)) }
sub index_8302 { index_data_field(@_, '830', qw(p)) }

sub index_ISB3 {
    my @values = index_data_field(@_, '020', 'a');
    for (@values) {
        s/\s.*//;
        tr/- //d;
        if (length == 10) {
            $_ = '978' . substr($_, 0, 9);
            $_ .= isbn13_check_digit($_);
        }
    }
    return @values;
}

# The following have all been confirmed in Sys Admin > Search
# Though the order in which subfields are indexed cannot be confirmed there
sub index_008D { map { trim(substr($_,  7, 4)) } index_control_field(@_, '008') }
sub index_008L { map { trim(substr($_, 35, 3)) } index_control_field(@_, '008') }
sub index_008P { map { trim(substr($_, 15, 3)) } index_control_field(@_, '008') }

sub index_100H { index_data_field(@_, '100', qw(a b c d e f g k l n p q t u)) }
sub index_110H { index_data_field(@_, '110', qw(a b c d e f g k l n p t u)) }
sub index_111H { index_data_field(@_, '111', qw(a b c d e f g k l n p q t u)) }

sub index_1300 { index_data_field(@_, '130', qw(a d f g h k l m n o p r s t)) }
sub index_1301 { index_data_field(@_, '130', qw(a l n p s)) }
sub index_1302 { index_data_field(@_, '130', qw(p)) }

sub index_2100 { index_data_field(@_, '210', qw(a b)) }
sub index_2110 { index_data_field(@_, '210', qw(a b)) }
sub index_2120 { index_data_field(@_, '212', qw(a)) }  # Obsolete
sub index_2140 { index_data_field(@_, '214', qw(a)) }

sub index_2220 { index_title_field(@_, '222', qw(a b)) }

sub index_2400 { index_title_field(@_, '240', qw(a d f g h k l m n o p r s)) }
sub index_2401 { index_title_field(@_, '240', qw(a l n p s)) }
sub index_2402 { index_title_field(@_, '240', qw(p)) }

sub index_2450 { index_title_field(@_, '245', qw(a b f g h k n p s)) }
sub index_2451 { index_title_field(@_, '245', qw(a b)) }
sub index_2452 { index_title_field(@_, '245', qw(a b h)) }

sub index_2460 { index_data_field(@_, '246', qw(a b f g h n p)) }
sub index_2470 { index_data_field(@_, '247', qw(a b f g h n p x)) }
sub index_260D { index_data_field(@_, '260', qw(d)) }  # Obsolete

sub index_600H { index_data_field(@_, '600', qw(a b c d f g k l m n o p q r t v x y z)) }
sub index_600T { index_data_field(@_, '600', qw(t)) }
sub index_610H { index_data_field(@_, '610', qw(a b c d f g k l m n o p r s t v x y z)) }
sub index_611H { index_data_field(@_, '611', qw(a b c d e f g k l n p q s t v x y z)) }
sub index_630H { index_data_field(@_, '630', qw(a d f g k l m n o p r s v x y z)) }

sub index_data_field_where_ind2 {
    my $fields = shift;
    my $tag    = shift;
    my $ind2   = shift;
    my @fields = grep { $_->[TAG] eq $tag && $_->[IND2] eq $ind2 } @$fields;
    return index_data_field(\@fields, $tag, @_);
}

sub index_data_field {
    my $fields = shift;
    my $tag = shift;
    my $code;
    $code = pop if @_ && ref $_[-1] eq 'CODE';
    my @subfields = @_;
    my @fields = grep { $_->[TAG] eq $tag } @$fields;
    my @values;
    my %wanted = map { $_ => 1 } @subfields;
    foreach my $f (@fields) {
        my @subvalues;
        my $snum = 0;
        foreach my $sub (@$f[SUBS..$#$f]) {
            $snum++;
            my $id = $sub->[SUB_ID];
            next if !$wanted{$id};
            my $valref = $sub->[SUB_VALREF];
            my $value = $$valref;
            if ($code) {
                $code->() for $value;
            }
            else {
                $value = norm_value($$valref);
            }
            push @subvalues, $value if length $value;
        }
        next if !@subvalues;
        push @values, join(' ', @subvalues);
    }
    return @values;
}

sub index_data_field_each_subfield {
    my $fields = shift;
    my $tag = shift;
    my $code;
    $code = pop if @_ && ref $_[-1] eq 'CODE';
    my ($subfield) = @_;
    my @fields = grep { $_->[TAG] eq $tag } @$fields;
    my @values;
    foreach my $f (@fields) {
        foreach my $sub (@$f[SUBS..$#$f]) {
            my $id = $sub->[SUB_ID];
            next if $id ne $subfield;
            my $value = ${ $sub->[SUB_VALREF] };
            if ($code) {
                $code->() for $value;
            }
            else {
                $value = norm_value($value);
            }
            push @values, $value if length $value;
        }
    }
    return @values;
}

sub index_title_field {
    my ($fields, $tag, @subfields) = @_;
    my @fields = grep { $_->[TAG] eq $tag } @$fields;
    my @values;
    my %wanted = map { $_ => 1 } @subfields;
    foreach my $f (@fields) {
        my $ind2 = $f->[IND2];
        my $snum = 0;
        my @subvalues;
        foreach my $sub (@$f[SUBS..$#$f]) {
            $snum++;
            my $id = $sub->[SUB_ID];
            next if !$wanted{$id};
            my $valref = $sub->[SUB_VALREF];
            my $value;
            if ($snum == 1 && $id eq 'a' && $ind2 > 0) {
                $value = substr($$valref, $ind2);
            }
            else {
                $value = $$valref;
            }
            push @subvalues, norm_value($value);  # XXX Don't normalize here??
        }
        @subvalues = grep { length($_) } @subvalues;
        next if !@subvalues;
        push @values, join(' ', @subvalues);
    }
    return map { norm_value($_) } @values;
}

sub index_control_field {
    my ($fields, $tag) = @_;
    my @fields = grep { $_->[TAG] eq $tag } @$fields;
    map { ${ $_->[VALREF] } } @fields;
}

sub norm_value {
    my ($str) = @_;
    $str =~ tr{'}{}d;
    $str =~ tr{-=./()[],:;"?!}{ };
    $str =~ s/[^[:alpha] ]//g;
    $str =~ tr{ }{}s;
    $str =~ s/^ | $//g;
    $str =~ s/\xc5[\x81-\x82]/L/g;  # LATIN CAPITAL/SMALL LETTER L WITH STROKE
    $str =~ s/[^\x20-\x7e]//g;
    return substr(uc $str, 0, 150);
}

sub norm_control_value {
    my ($str) = @_;
    $str =~ tr{'}{}d;
    $str =~ tr{=./()[],:;"?}{ };
    $str =~ s/[^[:alpha] ]//g;
    $str =~ tr{ }{}s;
    $str =~ s/^ | $//g;
    $str =~ s/[^\x20-\x7e]//g;
    return substr(uc $str, 0, 150);
}

sub isbn13_check_digit {
    # Adapted from Algorithm::CheckDigits:M10_004 by Mathias Weidner
    my ($str) = @_;
    return '' if $str !~ /^([0-9]{12})$/;
    my @digits = split //, $str;
    my $even   = 1;
    my $sum    = 0;
    foreach my $digit (reverse @digits) {
        $sum += $even ? 3 * $digit : $digit;
        $even = !$even;
    }
    return ( 10 - $sum % 10 ) % 10;
}

sub trim {
    my ($str) = @_;
    $str =~ s/^\s+|\s+$//g;
    return $str;
}
