# /*
# PollMonster - a distributed data collection framework
# Copyright (C) 2010 Jerry Lundström
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# PollMonster is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with PollMonster.  If not, see <http://www.gnu.org/licenses/>.

=head1 NAME

PollMonster - The great new PollMonster!

=head1 VERSION

See L<PollMonster> for version.

=cut

=head1 SYNOPSIS

Quick summary of what the module does.

Perhaps a little code snippet.

    use PollMonster;

    my $foo = PollMonster->new();
    ...

=head1 EXPORT

A list of functions that can be exported.  You can delete this section
if you don't export anything, such as for a purely object-oriented module.

=head1 SUBROUTINES/METHODS

=head2 function1

=cut

package WorkerSNMP::XS;
our $VERSION = '0.01';

sub _SECTION (){ 'module worker-snmp-xs' }

no AutoLoader;

use strict;
no strict 'subs';
use warnings;

use base qw(Exporter);
our @EXPORT_OK = qw(
snmp_xs_get
snmp_xs_bulkget
snmp_xs_set
snmp_xs_walk
snmp_xs_bulkwalk
);

use PollMonster;

my @InlineConfig;

if (defined PollMonster->CFG(_SECTION, 'ccflags')) {
    push(@InlineConfig, CCFLAGS => PollMonster->CFG(_SECTION, 'ccflags'));
}

if (defined PollMonster->CFG(_SECTION, 'libs')) {
    push(@InlineConfig, LIBS => PollMonster->CFG(_SECTION, 'libs'));
}

if (defined PollMonster->CFG(_SECTION, 'inc')) {
    push(@InlineConfig, INC => PollMonster->CFG(_SECTION, 'inc'));
}

if (@InlineConfig) {
    require Inline;
    Inline->import(C, Config, @InlineConfig);
}

use Inline (C => 'DATA',
            #VERSION => '0.01',
            NAME => 'WorkerSNMP::XS');

Inline->init;

=head1 AUTHOR

Jerry Lundström, C<< <lundstrom.jerry at gmail.com> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-pollmonster at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=PollMonster>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.




=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc PollMonster


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=PollMonster>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/PollMonster>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/PollMonster>

=item * Search CPAN

L<http://search.cpan.org/dist/PollMonster/>

=back


=head1 ACKNOWLEDGEMENTS


=head1 LICENSE AND COPYRIGHT

Copyright 2010 Jerry Lundström.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

PollMonster is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with PollMonster.  If not, see <http://www.gnu.org/licenses/>.


=cut

1;

# */

__DATA__

__C__

#include <net-snmp/net-snmp-config.h>
#include <net-snmp/net-snmp-includes.h>

#ifdef HAVE_WINSOCK_H
#include <winsock.h>
#endif

extern int snmp_errno;
int __init = 0;

struct snmp_xs_oid {
    const char *name_str;
    oid name[MAX_OID_LEN];
    size_t name_length;
    int is_set;
};

#define SNMP_XS_GET      1
#define SNMP_XS_BULKGET  2
#define SNMP_XS_SET      3
#define SNMP_XS_WALK     4
#define SNMP_XS_BULKWALK 5

#define SNMP_XS_MAX_REPETITIONS 100

struct snmp_xs_session {
    const char *host;
    int host_len;
    struct snmp_session *sess;
    HV* hv_result;
    HV* hv_error;
    int *active_pdus;
    int type;
    void* magic;
};

struct snmp_xs_get_session {
    struct snmp_xs_oid *oids;
};

struct snmp_xs_walk_session {
    struct snmp_xs_oid *root_base_oid;
    struct snmp_xs_oid *root_oid;
};

struct snmp_xs_bulkwalk_session {
    struct snmp_xs_oid *root_base_oid;
    struct snmp_xs_oid *root_oid;
    struct snmp_xs_oid next_oid;
    int non_repeaters;
    int max_repetitions;
    HV* hv_next;
};

void __snmp_xs_initialize() {
    if (!__init) {
        SOCK_STARTUP;
        init_snmp("WorkerSNMP");
        snmp_out_toggle_options("eQvtn");
        __init = 1;
    }
}

void snmp_xs_get_response(struct snmp_pdu *pdu, struct snmp_xs_session *sess);
void snmp_xs_walk_response(struct snmp_pdu *pdu, struct snmp_xs_session *sess);
void snmp_xs_bulkwalk_response(struct snmp_pdu *pdu, struct snmp_xs_session *sess);

int snmp_xs_response(int operation, struct snmp_session *ssp, int reqid, struct snmp_pdu *pdu, void *magic) {
    struct snmp_xs_session *sp = (struct snmp_xs_session *)magic;
    struct variable_list *vp;
    int ix, err_len;
    SV* sv_value;
    char err[4096], name[128];

    switch (operation) {
    case NETSNMP_CALLBACK_OP_RECEIVED_MESSAGE:
        if (pdu->errstat == SNMP_ERR_NOERROR) {
            switch (sp->type) {
            case SNMP_XS_GET:
                snmp_xs_get_response(pdu, sp);
                break;

            case SNMP_XS_BULKGET:
                break;

            case SNMP_XS_SET:
                break;

            case SNMP_XS_WALK:
                snmp_xs_walk_response(pdu, sp);
                break;

            case SNMP_XS_BULKWALK:
                snmp_xs_bulkwalk_response(pdu, sp);
                break;

            default:
                if ((sv_value = newSViv(0))) {
                    hv_store(sp->hv_error, sp->host, sp->host_len, sv_value, -4);
                }
            }
        }
        else {
            vp = pdu->variables;

            for (ix = 1; vp && ix != pdu->errindex; vp = vp->next_variable, ix++)
                ;

            if (vp)
                snprint_objid(name, sizeof(name), vp->name, vp->name_length);
            else
                strcpy(name, "(none)");

            if ((err_len = snprintf(err, sizeof(err), "%s: %s: %s\n", sp->host, name, snmp_errstring(pdu->errstat)))) {
                if ((sv_value = newSVpv(err, err_len))) {
                    hv_store(sp->hv_error, sp->host, sp->host_len, sv_value, 0);
                }
            }
        }
        break;

	case NETSNMP_CALLBACK_OP_TIMED_OUT:
        if ((sv_value = newSViv(0))) {
            hv_store(sp->hv_error, sp->host, sp->host_len, sv_value, -2);
        }
        break;

	case NETSNMP_CALLBACK_OP_SEND_FAILED:
        if ((sv_value = newSViv(0))) {
            hv_store(sp->hv_error, sp->host, sp->host_len, sv_value, -3);
        }
        break;

	case NETSNMP_CALLBACK_OP_CONNECT:
	case NETSNMP_CALLBACK_OP_DISCONNECT:
        /* Don't know if we ever get these */
        return 1;
        break;

    default:
        if ((sv_value = newSViv(0))) {
            hv_store(sp->hv_error, sp->host, sp->host_len, sv_value, -1);
        }
    }

    (*(sp->active_pdus))--;

    return 1;
}

void snmp_xs_get_response(struct snmp_pdu *pdu, struct snmp_xs_session *sp) {
    struct snmp_xs_get_session *gsp = (struct snmp_xs_get_session *)(sp->magic);
    struct snmp_xs_oid *op;
    char value[4096], name[128];
    struct variable_list *vp;
    int ix, name_len, value_len, have;
    AV* av_varbind_list;
    SV* sv_value;

    av_varbind_list = newAV();

    for (op=gsp->oids; op->name_str; op++) {
        vp = pdu->variables;
        have = 0;

        while (vp) {
            if (vp->type != SNMP_NOSUCHOBJECT &&
                vp->type != SNMP_NOSUCHINSTANCE &&
                op->name_length == vp->name_length &&
                !memcmp(op->name, vp->name, op->name_length * sizeof(oid)) &&
                (value_len = snprint_variable(value, sizeof(value), vp->name, vp->name_length, vp)) > 0 &&
                (sv_value = newSVpv(value, value_len)))
            {
                av_push(av_varbind_list, sv_value);
                have = 1;
                break;
            }
            vp = vp->next_variable;
        }

        if (!have) {
            av_push(av_varbind_list, newSV(0));
        }
    }

    hv_store(sp->hv_result, sp->host, sp->host_len, newRV_noinc((SV*)av_varbind_list), 0);
}

void snmp_xs_get(int version, char* community, int timeout, int retries, SV* oids_ref, SV* hosts_ref, SV* result_ref, SV* error_ref) {
    struct snmp_xs_session *sessions, *sp;
    struct snmp_xs_get_session *get_sessions, *gsp;
    struct snmp_xs_oid *oids, *op;
    int active_pdus, i, len, community_len;
    AV* av_hosts;
    AV* av_oids;
    HV* hv_result, *hv_error;
    SV** svp, *sv_value;

    __snmp_xs_initialize();
    
    /* Check oids */
    if (!SvROK(oids_ref)) {
        warn("oids_ref is not a reference");
        return;
    }

    av_oids = (AV*)SvRV(oids_ref);

    if (SvTYPE(av_oids) != SVt_PVAV) {
        warn("oids_ref is not an array");
        return;
    }

    if (av_len(av_oids) < 0) {
        warn("no oids giving in av_oids");
        return;
    }

    /* Check hosts */
    if (!SvROK(hosts_ref)) {
        warn("hosts_ref is not a reference");
        return;
    }

    av_hosts = (AV*)SvRV(hosts_ref);

    if (SvTYPE(av_hosts) != SVt_PVAV) {
        warn("hosts_ref is not an array");
        return;
    }

    if (av_len(av_hosts) < 0) {
        warn("no hosts found in av_hosts");
        return;
    }

    /* Check result */
    if (!SvROK(result_ref)) {
        warn("result_ref is not a reference");
        return;
    }

    hv_result = (HV*)SvRV(result_ref);

    if (SvTYPE(hv_result) != SVt_PVHV) {
        warn("result_ref is not an hash");
        return;
    }

    /* Check error */
    if (!SvROK(error_ref)) {
        warn("error_ref is not a reference");
        return;
    }

    hv_error = (HV*)SvRV(error_ref);

    if (SvTYPE(hv_error) != SVt_PVHV) {
        warn("error_ref is not an hash");
        return;
    }

    /* Setup oids */
    len = av_len(av_oids)+1;
    if (!(oids = calloc(len+1, sizeof(struct snmp_xs_oid)))) {
        warn("calloc oids failed");
        return;
    }

    for (op=oids, i=0; i<len; i++, op++) {
        if (!(svp = av_fetch(av_oids, i, 0))) {
            free(oids);
            warn("can not fetch oid");
            return;
        }

        op->name_str = SvPV_nolen(*svp);
    }

    for (op=oids; op->name_str; op++) {
        op->name_length = MAX_OID_LEN;
        if (!read_objid(op->name_str, op->name, &op->name_length)) {
            free(oids);
            warn("error when read_objid: %s", snmp_errstring(snmp_errno));
            return;
        }
    }

    /* Setup sessions */
    len = av_len(av_hosts)+1;
    if (!(sessions = calloc(len+1, sizeof(struct snmp_xs_session)))) {
        free(oids);
        warn("calloc sessions failed");
        return;
    }

    if (!(get_sessions = calloc(len+1, sizeof(struct snmp_xs_get_session)))) {
        free(oids);
        free(sessions);
        warn("calloc get_sessions failed");
        return;
    }

    for (sp=sessions, gsp=get_sessions, i=0; i<len; i++, sp++, gsp++) {
        if (!(svp = av_fetch(av_hosts, i, 0))) {
            free(oids);
            free(sessions);
            free(get_sessions);
            warn("can not fetch host");
            return;
        }

        gsp->oids = oids;

        sp->type = SNMP_XS_GET;
        sp->host = SvPV_nolen(*svp);
        sp->host_len = strlen(sp->host);
        sp->active_pdus = &active_pdus;
        sp->hv_result = hv_result;
        sp->hv_error = hv_error;
        sp->magic = gsp;
    }

    community_len = strlen(community);
    for (active_pdus=0, sp=sessions; sp->host; sp++) {
        struct snmp_pdu *pdu;
        struct snmp_session sess;

        snmp_sess_init(&sess);
        switch (version) {
        case 1:
            sess.version = SNMP_VERSION_1;
            break;

        case 2:
            sess.version = SNMP_VERSION_2c;
            break;

        default:
            free(oids);
            free(sessions);
            free(get_sessions);
            warn("unknown version");
            return;
        }
        sess.peername = sp->host;
        sess.community = community;
        sess.community_len = community_len;
        sess.callback = snmp_xs_response;
        sess.callback_magic = sp;
        sess.timeout = timeout * 1000000L;
        sess.retries = retries;

        if (!(sp->sess = snmp_open(&sess))) {
            continue;
        }

        pdu = snmp_pdu_create(SNMP_MSG_GET);

        for (op=oids; op->name_str; op++) {
            snmp_add_null_var(pdu, op->name, op->name_length);
        }

        if (snmp_send(sp->sess, pdu))
            active_pdus++;
        else {
            if ((sv_value = newSViv(0))) {
                hv_store(hv_error, sp->host, sp->host_len, sv_value, 0);
            }

            snmp_free_pdu(pdu);
        }
    }

    /* Run it */
    while (active_pdus) {
        int fds = 0, block = 1;
        fd_set fdset;
        struct timeval tv_timeout;

        FD_ZERO(&fdset);
        snmp_select_info(&fds, &fdset, &tv_timeout, &block);
        fds = select(fds, &fdset, NULL, NULL, block ? NULL : &tv_timeout);
        if (fds < 0) {
            break;
        }
        if (fds)
            snmp_read(&fdset);
        else
            snmp_timeout();
    }

    for (sp=sessions; sp->host; sp++) {
        if (sp->sess)
            snmp_close(sp->sess);
    }

    free(oids);
    free(sessions);
    free(get_sessions);
    return;
}

void snmp_xs_bulkget() {
    warn("not implemented yet");
}

void snmp_xs_set() {
    warn("not implemented yet");
}

void snmp_xs_walk_response(struct snmp_pdu *pdu, struct snmp_xs_session *sp) {
    struct snmp_xs_walk_session *wsp = (struct snmp_xs_walk_session *)(sp->magic);
    struct snmp_xs_oid *op;
    char value[4096], name_str[128];
    struct variable_list *vp;
    int ix, name_str_len, value_len;
    HV* hv_varbind_list;
    SV* sv_value, **sv_tmp;
    oid name[MAX_OID_LEN];
    size_t name_length;

    hv_varbind_list = 0;

    if (hv_exists(sp->hv_result, sp->host, sp->host_len)) {
        if ((sv_tmp = hv_fetch(sp->hv_result, sp->host, sp->host_len, 0))) {
            hv_varbind_list = (HV*)SvRV(*sv_tmp);
        }
        else {
            hv_delete(sp->hv_result, sp->host, sp->host_len, G_DISCARD);
        }
    }

    if (!hv_varbind_list) {
        hv_varbind_list = newHV();
        hv_store(sp->hv_result, sp->host, sp->host_len, newRV_noinc((SV*)hv_varbind_list), 0);
    }

    name_length = 0;

    for (vp = pdu->variables; vp; vp = vp->next_variable) {
        if (vp->name_length < wsp->root_base_oid->name_length ||
            memcmp(wsp->root_base_oid->name, vp->name, wsp->root_base_oid->name_length * sizeof(oid)))
        {
            /* not part of this subtree */
            continue;
        }

        if (vp->type != SNMP_ENDOFMIBVIEW &&
            vp->type != SNMP_NOSUCHOBJECT &&
            vp->type != SNMP_NOSUCHINSTANCE)
        {
            if (vp->name_length > wsp->root_base_oid->name_length &&
                (name_str_len = snprint_objid(name_str, sizeof(name_str), vp->name + wsp->root_base_oid->name_length, vp->name_length - wsp->root_base_oid->name_length)) > 0 &&
                (value_len = snprint_variable(value, sizeof(value), vp->name, vp->name_length, vp)) > 0 &&
                (sv_value = newSVpv(value, value_len)))
            {
                if (name_str[0] == '.') {
                    hv_store(hv_varbind_list, name_str+1, name_str_len-1, sv_value, 0);
                }
                else {
                    hv_store(hv_varbind_list, name_str, name_str_len, sv_value, 0);
                }
            }

            memcpy(name, vp->name, vp->name_length * sizeof(oid));
            name_length = vp->name_length;
        }
    }

    if (name_length) {
        struct snmp_pdu *pdu;

        pdu = snmp_pdu_create(SNMP_MSG_GETNEXT);
        snmp_add_null_var(pdu, name, name_length);

        if (snmp_send(sp->sess, pdu))
            (*(sp->active_pdus))++;
        else {
            if ((sv_value = newSViv(0))) {
                hv_store(sp->hv_error, sp->host, sp->host_len, sv_value, 0);
            }

            snmp_free_pdu(pdu);
        }
    }
}

void snmp_xs_walk(char* community, int timeout, int retries, char* root_base_objid, char* root_objid, SV* hosts_ref, SV* result_ref, SV* error_ref) {
    struct snmp_xs_session *sessions, *sp;
    struct snmp_xs_walk_session *walk_sessions, *wsp;
    struct snmp_xs_oid *root_base_oid, *root_oid;
    int active_pdus, i, len, community_len;
    AV* av_hosts;
    AV* av_oids;
    HV* hv_result, *hv_error;
    SV** svp, *sv_value;

    __snmp_xs_initialize();

    /* Check hosts */
    if (!SvROK(hosts_ref)) {
        warn("hosts_ref is not a reference");
        return;
    }

    av_hosts = (AV*)SvRV(hosts_ref);

    if (SvTYPE(av_hosts) != SVt_PVAV) {
        warn("hosts_ref is not an array");
        return;
    }

    if (av_len(av_hosts) < 0) {
        warn("no hosts found in av_hosts");
        return;
    }

    /* Check result */
    if (!SvROK(result_ref)) {
        warn("result_ref is not a reference");
        return;
    }

    hv_result = (HV*)SvRV(result_ref);

    if (SvTYPE(hv_result) != SVt_PVHV) {
        warn("result_ref is not an hash");
        return;
    }

    /* Check error */
    if (!SvROK(error_ref)) {
        warn("error_ref is not a reference");
        return;
    }

    hv_error = (HV*)SvRV(error_ref);

    if (SvTYPE(hv_error) != SVt_PVHV) {
        warn("error_ref is not an hash");
        return;
    }

    /* Setup base oid */
    if (!(root_base_oid = calloc(1, sizeof(struct snmp_xs_oid)))) {
        warn("calloc base oid failed");
        return;
    }

    root_base_oid->name_str = root_base_objid;
    root_base_oid->name_length = MAX_OID_LEN;
    if (!read_objid(root_base_oid->name_str, root_base_oid->name, &root_base_oid->name_length)) {
        free(root_base_oid);
        warn("error when read_objid: %s", snmp_errstring(snmp_errno));
        return;
    }

    /* Setup oid */
    if (!(root_oid = calloc(1, sizeof(struct snmp_xs_oid)))) {
        free(root_base_oid);
        warn("calloc oids failed");
        return;
    }

    root_oid->name_str = root_objid;
    root_oid->name_length = MAX_OID_LEN;
    if (!read_objid(root_oid->name_str, root_oid->name, &root_oid->name_length)) {
        free(root_base_oid);
        free(root_oid);
        warn("error when read_objid: %s", snmp_errstring(snmp_errno));
        return;
    }

    /* Setup sessions */
    len = av_len(av_hosts)+1;
    if (!(sessions = calloc(len+1, sizeof(struct snmp_xs_session)))) {
        free(root_base_oid);
        free(root_oid);
        warn("calloc sessions failed");
        return;
    }

    len = av_len(av_hosts)+1;
    if (!(walk_sessions = calloc(len+1, sizeof(struct snmp_xs_walk_session)))) {
        free(root_base_oid);
        free(root_oid);
        free(sessions);
        warn("calloc sessions failed");
        return;
    }

    for (sp=sessions, wsp=walk_sessions, i=0; i<len; i++, sp++, wsp++) {
        if (!(svp = av_fetch(av_hosts, i, 0))) {
            free(root_base_oid);
            free(root_oid);
            free(sessions);
            free(walk_sessions);
            warn("can not fetch host");
            return;
        }

        wsp->root_base_oid = root_base_oid;
        wsp->root_oid = root_oid;

        sp->type = SNMP_XS_WALK;
        sp->host = SvPV_nolen(*svp);
        sp->host_len = strlen(sp->host);
        sp->active_pdus = &active_pdus;
        sp->hv_result = hv_result;
        sp->hv_error = hv_error;
        sp->magic = wsp;
    }

    community_len = strlen(community);
    for (active_pdus=0, sp=sessions; sp->host; sp++) {
        struct snmp_pdu *pdu;
        struct snmp_session sess;

        snmp_sess_init(&sess);
        sess.version = SNMP_VERSION_2c;
        sess.peername = sp->host;
        sess.community = community;
        sess.community_len = community_len;
        sess.callback = snmp_xs_response;
        sess.callback_magic = sp;
        sess.timeout = timeout * 1000000L;
        sess.retries = retries;

        if (!(sp->sess = snmp_open(&sess))) {
            continue;
        }

        pdu = snmp_pdu_create(SNMP_MSG_GETNEXT);
        snmp_add_null_var(pdu, root_oid->name, root_oid->name_length);

        if (snmp_send(sp->sess, pdu))
            active_pdus++;
        else {
            if ((sv_value = newSViv(0))) {
                hv_store(hv_result, sp->host, sp->host_len, sv_value, 0);
            }

            snmp_free_pdu(pdu);
        }
    }

    /* Run it */
    while (active_pdus) {
        int fds = 0, block = 1;
        fd_set fdset;
        struct timeval tv_timeout;

        FD_ZERO(&fdset);
        snmp_select_info(&fds, &fdset, &tv_timeout, &block);
        fds = select(fds, &fdset, NULL, NULL, block ? NULL : &tv_timeout);
        if (fds < 0) {
            break;
        }
        if (fds)
            snmp_read(&fdset);
        else
            snmp_timeout();
    }

    for (sp=sessions; sp->host; sp++) {
        if (sp->sess)
            snmp_close(sp->sess);
    }

    free(root_base_oid);
    free(root_oid);
    free(sessions);
    free(walk_sessions);
    return;
}

void snmp_xs_bulkwalk_response(struct snmp_pdu *pdu, struct snmp_xs_session *sp) {
    struct snmp_xs_bulkwalk_session *bwsp = (struct snmp_xs_bulkwalk_session *)(sp->magic);
    struct snmp_xs_oid *op;
    char value[4096], name[128];
    struct variable_list *vp;
    int ix, name_len, value_len;
    HV* hv_varbind_list;
    SV* sv_value, **sv_tmp;

    hv_varbind_list = 0;

    if (hv_exists(sp->hv_result, sp->host, sp->host_len)) {
        if ((sv_tmp = hv_fetch(sp->hv_result, sp->host, sp->host_len, 0))) {
            hv_varbind_list = (HV*)SvRV(*sv_tmp);
        }
        else {
            hv_delete(sp->hv_result, sp->host, sp->host_len, G_DISCARD);
        }
    }

    if (!hv_varbind_list) {
        hv_varbind_list = newHV();
        hv_store(sp->hv_result, sp->host, sp->host_len, newRV_noinc((SV*)hv_varbind_list), 0);
    }

    bwsp->next_oid.is_set = 0;

    for (vp = pdu->variables; vp; vp = vp->next_variable) {
        if (vp->name_length < bwsp->root_base_oid->name_length ||
            memcmp(bwsp->root_base_oid->name, vp->name, bwsp->root_base_oid->name_length * sizeof(oid)))
        {
            /* not part of this subtree */
            continue;
        }

        if (vp->type != SNMP_ENDOFMIBVIEW &&
            vp->type != SNMP_NOSUCHOBJECT &&
            vp->type != SNMP_NOSUCHINSTANCE)
        {
            if (vp->name_length > bwsp->root_base_oid->name_length &&
                (name_len = snprint_objid(name, sizeof(name), vp->name + bwsp->root_base_oid->name_length, vp->name_length - bwsp->root_base_oid->name_length)) > 0 &&
                (value_len = snprint_variable(value, sizeof(value), vp->name, vp->name_length, vp)) > 0 &&
                (sv_value = newSVpv(value, value_len)))
            {
                if (name[0] == '.') {
                    hv_store(hv_varbind_list, name+1, name_len-1, sv_value, 0);
                }
                else {
                    hv_store(hv_varbind_list, name, name_len, sv_value, 0);
                }
            }

            if (bwsp->max_repetitions > 0)
                bwsp->max_repetitions--;

            if (vp->next_variable == NULL) {
                memcpy(bwsp->next_oid.name, vp->name, vp->name_length * sizeof(oid));
                bwsp->next_oid.name_length = vp->name_length;
                bwsp->next_oid.is_set = 1;
            }
        }
    }

    if (bwsp->next_oid.is_set) {
        if (bwsp->max_repetitions == -1 || bwsp->max_repetitions > 0) {
            struct snmp_pdu *pdu;

            pdu = snmp_pdu_create(SNMP_MSG_GETBULK);
            pdu->non_repeaters = bwsp->non_repeaters;
            pdu->max_repetitions = SNMP_XS_MAX_REPETITIONS;
            snmp_add_null_var(pdu, bwsp->next_oid.name, bwsp->next_oid.name_length);

            if (snmp_send(sp->sess, pdu))
                (*(sp->active_pdus))++;
            else {
                if ((sv_value = newSViv(0))) {
                    hv_store(sp->hv_error, sp->host, sp->host_len, sv_value, 0);
                }

                snmp_free_pdu(pdu);
            }
        }
        else {
            if ((name_len = snprint_objid(name, sizeof(name), bwsp->next_oid.name, bwsp->next_oid.name_length)) > 0 &&
                (sv_value = newSVpv(name, name_len)))
            {
                hv_store(bwsp->hv_next, sp->host, sp->host_len, sv_value, 0);
            }
        }
    }
}

void snmp_xs_bulkwalk(char* community, int timeout, int retries, int non_repeaters, int max_repetitions, char* root_base_objid, char* root_objid, SV* hosts_ref, SV* result_ref, SV* error_ref, SV* next_ref) {
    struct snmp_xs_session *sessions, *sp;
    struct snmp_xs_bulkwalk_session *bulkwalk_sessions, *bwsp;
    struct snmp_xs_oid *root_base_oid, *root_oid;
    int active_pdus, i, len, community_len;
    AV* av_hosts;
    AV* av_oids;
    HV* hv_result, *hv_error, *hv_next;
    SV** svp, *sv_value;

    __snmp_xs_initialize();

    /* Check hosts */
    if (!SvROK(hosts_ref)) {
        warn("hosts_ref is not a reference");
        return;
    }

    av_hosts = (AV*)SvRV(hosts_ref);

    if (SvTYPE(av_hosts) != SVt_PVAV) {
        warn("hosts_ref is not an array");
        return;
    }

    if (av_len(av_hosts) < 0) {
        warn("no hosts found in av_hosts");
        return;
    }

    /* Check result */
    if (!SvROK(result_ref)) {
        warn("result_ref is not a reference");
        return;
    }

    hv_result = (HV*)SvRV(result_ref);

    if (SvTYPE(hv_result) != SVt_PVHV) {
        warn("result_ref is not an hash");
        return;
    }

    /* Check error */
    if (!SvROK(error_ref)) {
        warn("error_ref is not a reference");
        return;
    }

    hv_error = (HV*)SvRV(error_ref);

    if (SvTYPE(hv_error) != SVt_PVHV) {
        warn("error_ref is not an hash");
        return;
    }

    /* Check next */
    if (!SvROK(next_ref)) {
        warn("next_ref is not a reference");
        return;
    }

    hv_next = (HV*)SvRV(next_ref);

    if (SvTYPE(hv_next) != SVt_PVHV) {
        warn("next_ref is not an hash");
        return;
    }

    /* Setup base oid */
    if (!(root_base_oid = calloc(1, sizeof(struct snmp_xs_oid)))) {
        warn("calloc base oid failed");
        return;
    }

    root_base_oid->name_str = root_base_objid;
    root_base_oid->name_length = MAX_OID_LEN;
    if (!read_objid(root_base_oid->name_str, root_base_oid->name, &root_base_oid->name_length)) {
        free(root_base_oid);
        warn("error when read_objid: %s", snmp_errstring(snmp_errno));
        return;
    }

    /* Setup oid */
    if (!(root_oid = calloc(1, sizeof(struct snmp_xs_oid)))) {
        free(root_base_oid);
        warn("calloc oid failed");
        return;
    }

    root_oid->name_str = root_objid;
    root_oid->name_length = MAX_OID_LEN;
    if (!read_objid(root_oid->name_str, root_oid->name, &root_oid->name_length)) {
        free(root_base_oid);
        free(root_oid);
        warn("error when read_objid: %s", snmp_errstring(snmp_errno));
        return;
    }

    /* Setup sessions */
    len = av_len(av_hosts)+1;
    if (!(sessions = calloc(len+1, sizeof(struct snmp_xs_session)))) {
        free(root_base_oid);
        free(root_oid);
        warn("calloc sessions failed");
        return;
    }

    len = av_len(av_hosts)+1;
    if (!(bulkwalk_sessions = calloc(len+1, sizeof(struct snmp_xs_bulkwalk_session)))) {
        free(root_base_oid);
        free(root_oid);
        free(sessions);
        warn("calloc sessions failed");
        return;
    }

    for (sp=sessions, bwsp=bulkwalk_sessions, i=0; i<len; i++, sp++, bwsp++) {
        if (!(svp = av_fetch(av_hosts, i, 0))) {
            free(root_base_oid);
            free(root_oid);
            free(sessions);
            free(bulkwalk_sessions);
            warn("can not fetch host");
            return;
        }

        bwsp->root_base_oid = root_base_oid;
        bwsp->root_oid = root_oid;
        bwsp->max_repetitions = max_repetitions;
        bwsp->non_repeaters = non_repeaters;
        bwsp->hv_next = hv_next;

        sp->type = SNMP_XS_BULKWALK;
        sp->host = SvPV_nolen(*svp);
        sp->host_len = strlen(sp->host);
        sp->active_pdus = &active_pdus;
        sp->hv_result = hv_result;
        sp->hv_error = hv_error;
        sp->magic = bwsp;
    }

    community_len = strlen(community);
    for (active_pdus=0, sp=sessions; sp->host; sp++) {
        struct snmp_pdu *pdu;
        struct snmp_session sess;

        snmp_sess_init(&sess);
        sess.version = SNMP_VERSION_2c;
        sess.peername = sp->host;
        sess.community = community;
        sess.community_len = community_len;
        sess.callback = snmp_xs_response;
        sess.callback_magic = sp;
        sess.timeout = timeout * 1000000L;
        sess.retries = retries;

        if (!(sp->sess = snmp_open(&sess))) {
            continue;
        }

        pdu = snmp_pdu_create(SNMP_MSG_GETBULK);
        pdu->non_repeaters = non_repeaters;
        pdu->max_repetitions = SNMP_XS_MAX_REPETITIONS;
        snmp_add_null_var(pdu, root_oid->name, root_oid->name_length);

        if (snmp_send(sp->sess, pdu))
            active_pdus++;
        else {
            if ((sv_value = newSViv(0))) {
                hv_store(hv_result, sp->host, sp->host_len, sv_value, 0);
            }

            snmp_free_pdu(pdu);
        }
    }

    /* Run it */
    while (active_pdus) {
        int fds = 0, block = 1;
        fd_set fdset;
        struct timeval tv_timeout;

        FD_ZERO(&fdset);
        snmp_select_info(&fds, &fdset, &tv_timeout, &block);
        fds = select(fds, &fdset, NULL, NULL, block ? NULL : &tv_timeout);
        if (fds < 0) {
            break;
        }
        if (fds)
            snmp_read(&fdset);
        else
            snmp_timeout();
    }

    for (sp=sessions; sp->host; sp++) {
        if (sp->sess)
            snmp_close(sp->sess);
    }

    free(root_base_oid);
    free(root_oid);
    free(sessions);
    free(bulkwalk_sessions);
    return;
}
