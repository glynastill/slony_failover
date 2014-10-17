#!/usr/bin/perl

# Script:	failover.pl
# Copyright:	08/04/2012: v1.0.2 Glyn Astill <glyn@8kb.co.uk>
# Requires:	Perl 5.10.1+, Data::UUID, File::Slurp
#               PostgreSQL 9.0+ Slony-I 1.2+ / 2.0+
#
# This script is a command-line utility to manage switchover and failover
# of replication sets in Slony-I clusters.
#
# This script is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This script is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this script.  If not, see <http://www.gnu.org/licenses/>.

use strict;
use warnings;
use DBI;
use Getopt::Long qw/GetOptions/;
use Data::UUID;
use File::Slurp;
use v5.10.1;
use sigtrap 'handler' => \&sigExit, 'HUP', 'INT','ABRT','QUIT','TERM';
use Time::HiRes qw/usleep gettimeofday/;
use POSIX qw/strftime/;
use Config qw/%Config/;

use constant false => 0;
use constant true  => 1;

my $g_script_version = '1.0.2';
my $g_debug = false;
my $g_pidfile = '/var/run/slony_failover.pid';
my $g_pid_written = false;
my $g_lang = 'en';
my $g_prefix = '/tmp/slony_failovers';
my $g_separate_working = true;
my $g_log_prefix = '%t';
my $g_log_to_db = false;
my $g_logdb_name;
my $g_logdb_host;
my $g_logdb_port;
my $g_logdb_user;
my $g_logdb_pass;
my $g_slonikpath;
my $g_use_try_blocks = false;
my $g_lockset_method = 'multiple';
my $g_logfile = 'failover.log';
my $g_input;
my $g_silence_notice = false;
my $g_reason;
my $g_script;
my $g_node_from;
my $g_node_to;
my $g_clname;
my $g_dbname;
my $g_dbhost;
my $g_dbport = 5432;
my $g_dbconninfo;
my $g_dbpass = '';
my $g_dbuser = 'slony';
my $g_node_count;
my $g_available_node_count;
my $g_critical_node_count;
my $g_subs_follow_origin = false;
my $g_use_comment_aliases = false;
my @g_cluster;                      # Array refs of node info.  In hindsight this should have been a hash, should be fairly simple to switch.
my @g_failed;
my @g_sets;
my @g_lags;
my $g_result;
my $g_version;
my $g_failover_method = 'old';
my $g_resubscribe_method = 'subscribe';
my $g_failover = false;
my $g_fail_subonly = false;
my $g_drop_failed = false;
my $g_autofailover = false;
my $g_autofailover_poll_interval = 500;
my $g_autofailover_retry = 2;
my $g_autofailover_retry_sleep = 1000;
my $g_autofailover_provs = false;
my $g_autofailover_config_any = true;
my $g_autofailover_perspective_sleep = 20000;
my $g_autofailover_majority_only = false;
my $g_autofailover_is_quorum = false;
my @g_unresponsive;
my %g_backups;
my $g_pid = $$;


my %message = (
'en' => {
    'usage'                            => q{-h <host> -p <port> -db <database> -cl <cluster name> -u <username> -P <password> -f <config file> (Password option not recommended; use pgpass instead)},
    'title'                            => q{Slony-I failover script version $1},
    'cluster_fixed'                    => q{Aborting failover action: all origin nodes now responsive},
    'cluster_failed'                   => q{Found $1 failed nodes, sleeping for $2ms before retry $3 of $4},
    'load_cluster'                     => q{Getting a list of database nodes...}, 
    'load_cluster_fail'                => q{Unable to read cluster configuration $1}, 
    'load_cluster_success'             => q{Loaded Slony-I v$1 cluster "$2" with $3 nodes read from node at $4:$5/$6}, 
    'lag_detail'                       => q{Current node lag information from configuration node:},
    'script_settings'                  => q{Using $1 batches of lock set, $2 FAILOVER and $3},
    'generated_script'                 => q{Generated script "$1"},
    'autofailover_init'                => q{Entering autofailover mode},
    'autofailover_init_cnf'            => q{Slony configuration will be read from $1 node},
    'autofailover_init_pol'            => q{Polling every $1ms},
    'autofailover_init_ret'            => q{Failed nodes will be retried $1 times with $2ms sleep},
    'autofailover_init_set'            => q{Failed forwarding providers $1 be failed over},
    'autofailover_load_cluster'        => q{$1 Slony-I v$2 cluster "$3" with $4 nodes},
    'autofailover_proceed'             => q{Proceeding with failover:},
    'autofailover_detail'              => q{Failed node: $1, Backup node: $2},
    'autofailover_halt'                => q{Unable to perform any failover for $1 failed nodes},
    'autofailover_check_sub'           => q{Checking subscriber node $1},
    'autofailover_check_sub_fail'      => q{Unable to check subscriber node $1},
    'autofailover_promote_find'        => q{Finding most up to date subscriber to all sets ($1) on unresponsive node $2},
    'autofailover_promote_found'       => q{Using previously found most up to date subscriber to all sets ($1) on unresponsive node $2},
    'autofailover_promote_skip'        => q{No failover required for unresponsive node $1 as it is neither the origin or an active forwarder of any sets},
    'autofailover_promote_fail'        => q{Could not find suitable backup node for promotion},
    'autofailover_node_detail'         => q{Node $1 is $2 subscribed to ($3) node $4 and provides sets $5 at $6 lag ($7 events)},
    'autofailover_node_detail_subonly' => q{Node $1 is $2 subscribed to ($3) node $4 and is a subscriber only at $5 lag ($6 events)},
    'autofailover_promote_best'        => q{Best node for promotion is node $1 seq = $2 ($3 events)},
    'autofailover_unresponsive'        => q{Detected unresponsive provider node: $1},
    'autofailover_unresponsive_subonly'=> q{Detected unresponsive subscriber only node: $1},
    'autofailover_pspec_check_fail'    => q{Failed to connect to node $1: $2},
    'autofailover_pspec_check'         => q{Getting objective judgement from other nodes, apparent unresponsive nodes are : $1 (Failed nodes = $2 of $3)},
    'autofailover_pspec_check_sleep'   => q{Sleeping for $1 ms},
    'autofailover_pspec_check_data'    => q{$1: Node $2 says lag from node $3 -> $4 is $5 seconds},
    'autofailover_pspec_check_true'    => q{All detected failed nodes confirmed as lagging by other nodes},
    'autofailover_pspec_check_false'   => q{Not all nodes confirmed as lagging},
    'autofailover_pspec_check_unknown' => q{Unable to confirm lag status of all nodes},
    'autofailover_split_check'         => q{Surviving nodes ($1 of $2) are the majority},
    'autofailover_split_check_fail'    => q{Surviving nodes ($1) are not the majority},
    'interactive_head_id'              => q{ID},
    'interactive_head_name'            => q{Name},
    'interactive_head_status'          => q{Status},
    'interactive_head_providers'       => q{Provider IDs},
    'interactive_head_config'          => q{Configuration},
    'interactive_detail_1'             => q{Origin for sets: },
    'interactive_detail_2'             => q{Providing sets: },
    'interactive_detail_3'             => q{Subscriptions: },
    'interactive_choose_node'          => q{Please choose the node to move all sets $1:},
    'interactive_confirm'              => q{You chose to move sets $1 node $2 ($3). Is this correct [y/n]? },
    'interactive_action'               => q{Best course of action is most likely to do a "$1". Do you wish to continue [y/n]?},
    'interactive_surrender'            => q{Uable to determine best course of action},
    'interactive_write_script'         => q{Writing a script to $1 node $2 to $3},
    'interactive_check_nodes'          => q{Checking availability of database nodes...},
    'interactive_continue'             => q{Do you wish to continue [y/n]?},
    'interactive_drop_nodes'           => q{Do you want to also drop the failed nodes from the slony configuration [y/n]?},
    'interactive_preserve'             => q{Preserve subscription paths to follow the origin node (choose no if unsure) [y/n]?},
    'interactive_aliases'              => q{Generate aliases based on sl_node/set comments in parentheses (choose no if unsure) [y/n]?},
    'interactive_summary'              => q{Summary of nodes to be passed to failover:},
    'interactive_node_info'            => q{Node : $1 ($2) $3 (conninfo $4)},
    'interactive_run_script'           => q{Would you like to run this script now [y/n]?},
    'interactive_running'              => q{Running the script now. This may take some time; please be patient!},
    'interactive_reason'               => q{Please enter a brief reson for taking this action: },
    'interactive_failover_detail_1'    => q{Before you go any further please consider the impact of a full failover:},
    'interactive_failover_detail_2'    => q{The node you are failing over from will cease to participate in the cluster permanently until it is rebuild & subscribed},
    'interactive_failover_detail_3'    => q{If the outage is temporary (i.e. network/power/easily replaceable hardware related) consider waiting it out},
    'interactive_failover_detail_4'    => q{This type of failover is likely to be more a business decision than a technical one},
    'info_all_nodes_available'         => q{INFO: All nodes are available},
    'info_req_nodes_available'         => q{INFO: $1 of $2 nodes are available. No unavailable nodes are subscribed to the old origin},
    'wrn_node_unavailable'             => q{WARNING: Node $1 unavailable},
    'wrn_req_unavailable'              => q{WARNING: Old origin node ($1) is available, however $2 subscribers are unavailable},
    'wrn_not_tested'                   => q{WARNING: Script not tested with Slony-I v$1},
    'wrn_failover_issues'              => q{WARNING: Slony-I v$1 may struggle to failover correctly with multiple failed nodes (affects v2.0-2.1)},
    'note_autofail_fwd_only'           => q{NOTICE: Slony versions prior to 2.2 cannot initiate failover from only failed forwarding providers},
    'note_fail_sub_only'               => q{NOTICE: Slony versions prior to 2.2 cannot failover subscriber only nodes, reverting to failover_offline_subscriber_only = false},
    'note_multiple_try'                => q{NOTICE: Cannot lock multiple sets within try blocks in version $1 dropping back to single sets},
    'note_reshape_cluster'             => q{NOTICE: Either drop the failed subscribers or bring them back up, then retry to MOVE SET},
    'dbg_generic'                      => q{DEBUG: $1},
    'dbg_cluster'                      => q{DEBUG: NodeID $1/ProvIDs $2/Conninfo $3/OrigSets $4/NodeName $5/ProvTree $6/Active $7/FwdSets $8/ActSubSets $9},
    'dbg_resubscribe'                  => q{DEBUG: Checking possibility to resubscribe set $1 ($2) to node $3 ($4) which pulls $5 ($6) from $7 ($8)},
    'dbg_failover_method'              => q{DEBUG: Failover method is $1},
    'dbg_cluster_load'                 => q{DEBUG: Loading cluster configuration from $1},
    'dbg_cluster_good'                 => q{DEBUG: Cluster state good},
    'dbg_autofailover_check'           => q{DEBUG: Checking node $1 ($2) role is $3 (conninfo: $4)},
    'dbg_autofailover_active_check'    => q{DEBUG: Initiate active check of $1 node $2},
    'dbg_slonik_script'                => q{DEBUG: Running slonik script $1},
    'err_generic'                      => q{ERROR: $1},
    'err_no_database'                  => q{ERROR: Please specify a database name},
    'err_no_cluster'                   => q{ERROR: Please specify a slony cluster name},
    'err_no_host'                      => q{ERROR: Please specify a host},
    'err_no_config'                    => q{ERROR: No valid config found},
    'err_fail_config'                  => q{ERROR: Failed to load configuration},
    'err_write_fail'                   => q{ERROR: Could not write to $1 "$2"},
    'err_read_fail'                    => q{ERROR: Could not read from $1 "$2"},
    'err_unlink_fail'                  => q{ERROR: Could not delete $1 "$2"},
    'err_mkdir_fail'                   => q{ERROR: Unable to create $1 directory "$2"},
    'err_execute_fail'                 => q{ERROR: Could not execute $1 "$2"},
    'err_inactive'                     => q{ERROR: Node $1 is not active (state = $2)},
    'err_cluster_empty'                => q{ERROR: Loaded cluster contains no nodes}, 
    'err_cluster_offline'              => q{ERROR: Loaded cluster contains no reachable nodes}, 
    'err_cluster_lone'                 => q{ERROR: Loaded cluster contains only 1 node}, 
    'err_not_origin'                   => q{ERROR: Node $1 is not the origin of any sets},
    'err_not_provider'                 => q{ERROR: Node $1 is not a provider of any sets},
    'err_not_provider_sets'            => q{ERROR: Node $1 does not provide the sets required: need ($2) but provides ($3)},
    'err_no_configuration'             => q{ERROR: Could not read configuration for node $1},
    'err_must_enter_node_id'           => q{ERROR: You must enter a node id},
    'err_not_a_node_id'                => q{ERROR: I have no knowledge of a node $1},
    'err_same_node'                    => q{ERROR: Cant move from and to the same node},
    'err_node_offline'                 => q{ERROR: $1 node ($2) is not available},
    'err_incomplete_preamble'          => q{ERROR: Incomplete preamble},
    'err_running_slonik'               => q{ERROR: Could not run slonik: $1},
    'err_pgsql_connect'                => q{ERROR: Cannot connect to postgres server},
    'slonik_output'                    => q{SLONIK: $1},
    'exit_noaction'                    => q{Exiting, no action has been taken},
    'exit'                             => q{Exited by $1}
    },
'fr' => {
    'usage'                            => q{-h <host> -p <port> -db <database> -cl <cluster name> -u <username> -P <password> -f <config file> (Option mot de passe pas recommandé; utiliser pgpass place)},
    'title'                            => q{Slony-I failover (basculement) version de script $1},
    'cluster_fixed'                    => q{Abandon de l'action de basculement: tous les noeuds d'origine maintenant sensible},
    'cluster_failed'                   => q{Trouvé $1 échoué noeuds, couchage pour $2 ms avant réessayer $3 de $4},
    'load_cluster'                     => q{Obtenir une liste de noeuds de base de donnees...},
    'load_cluster_fail'                => q{Impossible de lire la configuration du cluster $1},
    'load_cluster_success'             => q{Chargé Slony-I v$1 groupe "$2" avec $3 noeuds lire à partir du noeud à $4:$5/$6},
    'lag_detail'                       => q{Current informations noeud de décalage à partir du noeud de configuration:},
    'script_settings'                  => q{Utilisation de $1 lots de système de verrouillage, $2 FAILOVER et $3},
    'generated_script'                 => q{Script généré "$1"},
    'autofailover_init'                => q{Entrer dans le mode de autofailover},
    'autofailover_init_cnf'            => q{Configuration Slony sera lu à partir de $1 noeud},
    'autofailover_init_pol'            => q{Vérifier toutes les $1ms},
    'autofailover_init_ret'            => q{Noeuds défaillants seront rejugés $1 fois avec $2 ms sommeil},
    'autofailover_init_set'            => q{Fournisseurs d'expédition échoué $1 être échoué sur},
    'autofailover_load_cluster'        => q{$1 Slony-I v$2 grappe "$3" avec $4 noeuds},
    'autofailover_proceed'             => q{De procéder à failover:},
    'autofailover_detail'              => q{Noeud défaillant: $1, noeud de sauvegarde: $2},
    'autofailover_halt'                => q{Noeuds Impossible d'effectuer une failover pour $1 échoué},
    'autofailover_check_sub'           => q{Vérification noeud abonné $1},
    'autofailover_check_sub_fail'      => q{Impossible de vérifier noeud abonné $1},
    'autofailover_promote_find'        => q{Trouver plus à jour abonné à tous les jeux ($1) sur le noeud ne répond pas $2},
    'autofailover_promote_found'       => q{Utilisation précédemment trouvé plus à jour abonné à tous les jeux ($1) sur le noeud ne répond pas $2},
    'autofailover_promote_skip'        => q{Pas de failover requis pour le noeud ne répond pas $1 car il n'est ni l'origine ou un transitaire active de tous les jeux},
    'autofailover_promote_fail'        => q{Impossible de trouver le noeud de sauvegarde approprié pour la promotion},
    'autofailover_node_detail'         => q{Noeud $1 est souscrit à $2 ($3) noeud $4 et fournit des ensembles de $5 à retard $6 ($7  événements)},
    'autofailover_node_detail_subonly' => q{Noeud $1 est souscrit à $2 ($3) et le noeud $4 est un abonné à retard $5 ($6 événements)},
    'autofailover_promote_best'        => q{Meilleur noeud pour la promotion est noeud $1 suivants = $2 ($3 événements)},
    'autofailover_unresponsive'        => q{Noeud ne répond pas détecté: $1},
    'autofailover_unresponsive_subonly'=> q{Abonné ne répond pas détecté seulement de noeud: $1},
    'autofailover_pspec_check_fail'    => q{Impossible de se connecter au noeud $1: $2},
    'autofailover_pspec_check'         => q{Obtenir un jugement objectif à partir d'autres noeuds, les noeuds qui ne répondent pas apparentes sont : $1 (Noeuds défaillants = $2 de $3)},
    'autofailover_pspec_check_sleep'   => q{Dormir pour $1 ms},
    'autofailover_pspec_check_data'    => q{$1: Noeud $2 dit décalage de $3 -> $4 noeud est $5 secondes},
    'autofailover_pspec_check_true'    => q{Tous les noeuds détectés pas confirmés comme à la traîne par d'autres noeuds},
    'autofailover_pspec_check_false'   => q{Pas tous les noeuds confirmé retard},
    'autofailover_pspec_check_unknown' => q{Impossible de confirmer le statut de latence de tous les noeuds},
    'autofailover_split_check'         => q{Autres noeuds ($1 sur $2) sont la majorité},
    'autofailover_split_check_fail'    => q{Autres noeuds ($1) ne sont pas la majorité},
    'interactive_head_name'            => q{Nom},
    'interactive_head_status'          => q{Statut},
    'interactive_head_providers'       => q{Fournisseur IDs},
    'interactive_detail_1'             => q{Origine pour les jeux: },
    'interactive_detail_2'             => q{Fournir des ensembles: },
    'interactive_detail_3'             => q{Abonnements: },
    'interactive_choose_node'          => q{S'il vous plaît choisissez le noeud à déplacer tous les ensembles $1:},
    'interactive_confirm'              => q{Vous avez choisi de passer ensembles $1 noeud $2 ($3). Est-ce correct [o/n]? },
    'interactive_drop_nodes'           => q{Voulez-vous laisser tomber aussi les noeuds défaillants de la configuration de slony [o/n]?},
    'interactive_action'               => q{Meilleur plan d'action est le plus susceptible de faire une $1. Voulez-vous continuer [o/n]?},
    'interactive_surrender'            => q{Uable pour déterminer le meilleur plan d'action},
    'interactive_write_script'         => q{Rédaction d'un script à $1 $2 noeud à $3},
    'interactive_check_nodes'          => q{Vérification de la disponibilité des noeuds de base de donnees...},
    'interactive_continue'             => q{Voulez-vous continuer [o/n]?},
    'interactive_preserve'             => q{Préserver les chemins de souscription à suivre le noeud d'origine (ne pas choisir en cas de doute) [o/n]?},
    'interactive_aliases'              => q{Générer des alias sur la base de sl_node / set commentaires entre parenthèses (ne pas choisir en cas de doute) [o/n]?},
    'interactive_summary'              => q{Résumé des noeuds à passer à failover:},
    'interactive_node_info'            => q{Noeud : $1 ($2) $3 (conninfo $4)},
    'interactive_run_script'           => q{Voulez-vous exécuter ce script maintenant [o/n]?},
    'interactive_running'              => q{L'exécution du script maintenant. Cela peut prendre un certain temps; s'il vous plaît être patient!},
    'interactive_reason'               => q{S'il vous plaît entrer une brève reson pour cette action: },
    'interactive_failover_detail_1'    => q{Avant d'aller plus loin s'il vous plaît envisager l'impact d'un failover (basculement) complet:},
    'interactive_failover_detail_2'    => q{Le noeud vous ne parviennent pas au-dessus de cesse de participer au groupe de façon permanente jusqu'à ce qu'il soit à reconstruire et souscrit},
    'interactive_failover_detail_3'    => q{Si la panne est temporaire (c.-à-réseau / alimentation / facilement remplaçable matériel connexe) envisager d'attendre dehors},
    'interactive_failover_detail_4'    => q{Ce type de failover est susceptible d'être plus une décision d'affaires que technique},
    'info_all_nodes_available'         => q{INFO: Tous les noeuds sont disponibles},
    'info_req_nodes_available'         => q{INFO: $1 of $2 noeuds sont disponibles. Pas de noeuds indisponibles sont souscrites à l'ancienne origine},
    'wrn_node_unavailable'             => q{ATTENTION: Noeud $1 disponible},
    'wrn_req_unavailable'              => q{ATTENTION: Noeud Old origine ($1) est disponible, mais $2 abonnés ne sont pas disponibles},
    'wrn_not_tested'                   => q{ATTENTION: Script pas testé avec Slony-I v$1},
    'wrn_failover_issues'              => q{ATTENTION: Slony-I v$1 peut lutter pour basculer correctement avec plusieurs nœuds défaillants (affecte v2.0-2.1)},
    'note_autofail_fwd_only'           => q{AVIS: Versions antérieures à la 2.2 Slony ne peuvent pas initier le basculement de seulement échoué transmettre fournisseurs},
    'note_fail_sub_only'               => q{AVIS: Versions antérieures à la 2.2 Slony ne peuvent pas basculer abonnes seuls les noeuds, revenant à failover_offile_subscriber_only = false},
    'note_multiple_try'                => q{AVIS: Vous ne pouvez pas verrouiller plusieurs ensembles dans des blocs try dans la version $1 de retomber à des jeux simples},
    'note_reshape_cluster'             => q{AVIS: Vous devez supprimer les abonnés défaillants ou les ramener, puis réessayez à MOVE SET},
    'err_generic'                      => q{ERREUR: $1},
    'err_no_database'                  => q{ERREUR: S'il vous plaît spécifier un base de donnees nom},
    'err_no_cluster'                   => q{ERREUR: S'il vous plaît indiquez un nom de cluster slony},
    'err_no_host'                      => q{ERREUR: S'il vous plaît spécifier un hôte},
    'err_no_config'                    => q{ERREUR: Aucune configuration valide n'a été trouvée},
    'err_fail_config'                  => q{ERREUR: Impossible de charger la configuration},
    'err_write_fail'                   => q{ERREUR: Impossible d'écrire dans $1 "$2"},
    'err_read_fail'                    => q{ERREUR: Impossible de lire $1 "$2"},
    'err_unlink_fail'                  => q{ERREUR: Impossible de supprimer $1 "$2"},
    'err_mkdir_fail'                   => q{ERREUR: Impossible de créer $1 répertoire "$2"},
    'err_execute_fail'                 => q{ERREUR: Impossible d'exécuter $1 "$2"},
    'err_inactive'                     => q{ERREUR: Noeud $1 n'est pas active (état = $2)},
    'err_cluster_empty'                => q{ERREUR: Groupe chargé contient pas de noeuds},
    'err_cluster_offline'              => q{ERREUR: Groupe chargé contient pas de noeuds accessibles},
    'err_cluster_lone'                 => q{ERRRUE: Groupe chargé ne contient que 1 noeud},
    'err_not_origin'                   => q{ERREUR: Noeud $1 n'est pas à l'origine de tous les jeux},
    'err_not_provider'                 => q{ERREUR: Noeud $1 n'est pas un fournisseur de tous les jeux},
    'err_not_provider_sets'            => q{ERREUR: Noeud $1 ne fournit pas les ensembles nécessaires: le besoin ($2), mais fournit ($3)},
    'err_no_configuration'             => q{ERREUR: Impossible de lire la configuration pour le noeud $1},
    'err_must_enter_node_id'           => q{ERREUR: Vous devez entrer un id de noeud},
    'err_not_a_node_id'                => q{ERREUR: Je n'ai pas connaissance d'un $1 de noeud},
    'err_same_node'                    => q{ERREUR: Cant déplacer depuis et vers le même noeud},
    'err_node_offline'                 => q{ERREUR: $1 noeud ($2) n'est pas disponible},
    'err_incomplete_preamble'          => q{ERREUR: Préambule incomplète},
    'err_running_slonik'               => q{ERREUR: Ne pouvait pas courir slonik: $1},
    'err_pgsql_connect'                => q{ERREUR: Impossible de se connecter au serveur postgres},
    'slonik_output'                    => q{SLONIK: $1},
    'exit_noaction'                    => q{Quitter, aucune action n'a été prise},
    'exit'                             => q{Quitter par $1}
    }
);


# Setup date variables
my ($g_year, $g_month, $g_day, $g_hour, $g_min, $g_sec) = (localtime(time))[5,4,3,2,1,0];
my $g_date = sprintf ("%02d:%02d:%02d on %02d/%02d/%04d", $g_hour, $g_min, $g_sec, $g_day, $g_month+1, $g_year+1900);

# Handle command line options
Getopt::Long::Configure('no_ignore_case');
use vars qw{%opt};
die lookupMsg('usage') unless GetOptions(\%opt, 'host|H=s', 'port|p=i', 'dbname|db=s', 'clname|cl=s', 'dbuser|u=s', 'dbpass|P=s', 'cfgfile|f=s', 'infoprint|I', ) and keys %opt and ! @ARGV;

# Read configuration
if (defined($opt{cfgfile})) {
    unless (getConfig($opt{cfgfile})) {
        println(lookupMsg('err_no_config'));
        exit(1);
    }
}
else {
    if (defined($opt{dbname})) {
        $g_dbname = $opt{dbname};
    }
    if (defined($opt{clname})) {
        $g_clname = $opt{clname};
    }
    if (defined($opt{host})) {
        $g_dbhost = $opt{host};
    }
    if (defined($opt{port})) {
        $g_dbport = $opt{port};
    }
    if (defined($opt{dbuser})) {
        $g_dbuser = $opt{dbuser};
    }
    if (defined($opt{dbpass})) {
        $g_dbpass = $opt{dbpass};
    }
}

# Fill in any missing values with defaults or display message and die
if (!defined($g_dbname)) {
    println(lookupMsg('err_no_database'));
    die lookupMsg('usage');
}
if (!defined($g_clname)) {
    println(lookupMsg('err_no_cluster'));
    die lookupMsg('usage');
}
if (!defined($g_dbhost)) {
    println(lookupMsg('err_no_host'));
    die lookupMsg('usage');
}


# Build conninfo from supplied datbase name/host/port
$g_dbconninfo = "dbname=$g_dbname;host=$g_dbhost;port=$g_dbport";

if (!defined($opt{infoprint})) {
    # Check prefix directory and create if not present
    unless(-e $g_prefix or mkdir $g_prefix) {
        println(lookupMsg('err_mkdir_fail', 'prefix', $g_prefix));
        exit(2);
    }

    if ($g_separate_working) {
        if ($g_prefix !~ m/\/$/) {
            $g_prefix .= "/";
        }

        # Get a uuid for working directory
        $g_prefix .= getUUID($g_date);

        # Create a working directory and setup log file
        unless(-e $g_prefix or mkdir $g_prefix) {
            println(lookupMsg('err_mkdir_fail', 'work', $g_prefix));
        }
    }
}

# Set postgres path if provided
if (defined($g_slonikpath) && ($g_slonikpath ne "")) {
    $ENV{PATH} .= ":$g_slonikpath";
}

# Check if autofailover is enabled, if so check configuration and enter autofailover mode
if (($g_autofailover) && !defined($opt{infoprint})) {

    # Write out a PID file
    if (writePID($g_prefix, $g_logfile, $g_log_prefix, $g_pidfile)) {
        $g_pid_written = true;
    }
    else {
        cleanExit(1, "system");
    }
    
    # Go into endless loop for autofailover
    autoFailover($g_dbconninfo, $g_clname, $g_dbuser, $g_dbpass, $g_prefix, $g_logfile, $g_log_prefix);
}

# Read slony configuration and output some basic information
eval {
    #local $| = 1;
    println(lookupMsg('load_cluster', $g_prefix));
    ($g_node_count, $g_version) = loadCluster($g_dbconninfo, $g_clname, $g_dbuser, $g_dbpass, $g_prefix, $g_logfile, $g_log_prefix);
};
if ($@) {
    println(lookupMsg('load_cluster_fail', 'from supplied configuration'));
    cleanExit(2, "system");
}

if (defined($opt{infoprint})) {
    println(lookupMsg('load_cluster_success', $g_version, $g_clname, $g_node_count, $g_dbhost, $g_dbport, $g_dbname) . ":");
    chooseNode("info", undef, undef, undef, 0);
    exit(0);
}
else {
    printlog($g_prefix,$g_logfile,$g_log_prefix,"*"x68 . "\n* ");
    printlogln($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('title', $g_script_version));
    printlogln($g_prefix,$g_logfile,$g_log_prefix,"*"x68);
}

if ($g_node_count <= 0) {
    printlogln($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('err_cluster_empty'));
    cleanExit(3, "system");
}
else {
    printlogln($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('load_cluster_success', $g_version, $g_clname, $g_node_count, $g_dbhost, $g_dbport, $g_dbname));
    printlogln($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('script_settings', $g_lockset_method, $g_failover_method, uc($g_resubscribe_method)));
}

# Output lag information between each node and node configuration was read from
if (loadLag($g_dbconninfo, $g_clname, $g_dbuser, $g_dbpass, $g_prefix, $g_logfile, $g_log_prefix) > 0) {
    printlogln($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('lag_detail'));
    foreach (@g_lags) {
        printlogln($g_prefix,$g_logfile,$g_log_prefix,"\t$_");
    }
    printlog($g_prefix,$g_logfile,$g_log_prefix,"\n");
}

# Prompt user to choose nodes to move sets from / to
$g_node_from = chooseNode("from", $g_prefix, $g_logfile, $g_log_prefix, 0);
if ($g_node_from == 0) {
    cleanExit(4, "user");
}
elsif ($g_node_from == -1) {
    cleanExit(5, "system");
}

$g_node_to = chooseNode("to", $g_prefix, $g_logfile, $g_log_prefix, $g_node_from);
if ($g_node_to == 0) {
    cleanExit(6, "user");
}
elsif ($g_node_to == -1) {
    cleanExit(7, "system");
}
elsif ($g_node_from == $g_node_to) {
    printlogln($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('err_same_node'));
    cleanExit(8, "system");
}

# Check nodes are available and decide on action to take
printlogln($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('interactive_check_nodes'));
($g_available_node_count, $g_critical_node_count) = checkNodes($g_clname, $g_dbuser, $g_dbpass, $g_node_from, $g_node_to, $g_prefix, $g_logfile, $g_log_prefix);

if ($g_available_node_count <= 0) {
    printlogln($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('err_cluster_offline'));
    cleanExit(9, "system");
}
elsif ($g_critical_node_count == -1) {
    printlogln($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('err_node_offline', 'Target new origin', $g_node_to));
    cleanExit(10, "system");
}
elsif ($g_critical_node_count == -2) {
    printlogln($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('err_node_offline', 'Old origin', $g_node_from));
    printlog($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('interactive_action', 'FAILOVER'));
    $g_failover = true;
}
elsif ($g_critical_node_count == 0) {
    if ($g_node_count == $g_available_node_count) {
        printlogln($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('info_all_nodes_available'));
    }
    else {
        printlogln($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('info_req_nodes_available', $g_available_node_count, $g_node_count));
    }
    printlog($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('interactive_action', 'MOVE SET'));
}
elsif ($g_critical_node_count > 0) {
    printlogln($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('wrn_req_unavailable', $g_node_from, $g_critical_node_count));
    printlogln($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('note_reshape_cluster'));
    printlogln($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('exit_noaction'));
    cleanExit(11, "user");
}
else {
    printlog($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('interactive_surrender'));
    cleanExit(12, "system");
}
$g_input = <>;
chomp($g_input);
if ($g_input !~ /^[Y|O]$/i) {
    printlogln($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('exit_noaction'));
    cleanExit(13, "user");
}

if (!$g_use_comment_aliases) {
    printlog($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('interactive_aliases'));
    $g_input = <>;
    chomp($g_input);
    if ($g_input =~ /^[Y|O]$/i) {
        $g_use_comment_aliases = true;
    }
}

if ($g_failover) {
    printlogln($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('interactive_summary'));

    foreach (@g_failed) {
        printlogln($g_prefix,$g_logfile,$g_log_prefix,"\t" . lookupMsg('interactive_node_info',$_->[0],($_->[4] // "unnamed"),(defined($_->[9]) ? "providing sets $_->[9]" : "sole subscriber"), $_->[2])); 
    }

    printlogln($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('interactive_failover_detail_1'));
    printlogln($g_prefix,$g_logfile,$g_log_prefix,"\t" . lookupMsg('interactive_failover_detail_2'));
    printlogln($g_prefix,$g_logfile,$g_log_prefix,"\t" . lookupMsg('interactive_failover_detail_3'));
    printlogln($g_prefix,$g_logfile,$g_log_prefix,"\t" . lookupMsg('interactive_failover_detail_4'));

    printlog($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('interactive_drop_nodes'));
    $g_input = <>;
    if ($g_input ~~ /^[Y|O]$/i) {
        $g_drop_failed = true;
    }

    printlog($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('interactive_reason'));
    $g_reason = <>;
    printlog($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('interactive_continue'));
    $g_input = <>;
    chomp($g_input);
    if ($g_input !~ /^[Y|O]$/i) {
        printlogln($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('exit_noaction'));
        cleanExit(14, "user");
    }

    printlogln($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('interactive_write_script', 'failover from', $g_node_from, $g_node_to));
    $g_script = writeFailover($g_prefix, $g_dbconninfo, $g_clname, $g_dbuser, $g_dbpass, $g_node_from, $g_node_to, $g_subs_follow_origin, $g_use_comment_aliases, $g_logfile, $g_log_prefix);    
}
else {
    printlog($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('interactive_preserve'));
    $g_input = <>;
    chomp($g_input);
    if ($g_input =~ /^[Y|O]$/i) {
        $g_subs_follow_origin = true;
    }

    printlog($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('interactive_reason'));
    $g_reason = <>;

    printlogln($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('interactive_write_script', 'move all sets provided by', $g_node_from, $g_node_to));
    $g_script = writeMoveSet($g_prefix, $g_dbconninfo, $g_clname, $g_dbuser, $g_dbpass, $g_node_from, $g_node_to, $g_subs_follow_origin, $g_use_comment_aliases, $g_logfile, $g_log_prefix);    
}

# Complete and run script if required
if (-e $g_script) {
    printlogln($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('generated_script', $g_script));
    printlog($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('interactive_run_script', $g_script));
    $g_input = <>;
    chomp($g_input);
    if ($g_input =~ /^[Y|O]$/i) {
        printlogln($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('interactive_running'));
        unless (runSlonik($g_script, $g_prefix, $g_logfile, $g_log_prefix)) {
            printlogln($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('err_execute_fail', 'slonik script', $g_script));
        }
    }
    else {
        printlogln($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('exit_noaction'));
    }
}
else {
    printlogln($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('err_read_fail', 'slonik script', $g_script));
    cleanExit(15, "system");
}

cleanExit(0, "script completion");

###########################################################################################################################################

sub cleanExit {
    my $exit_code = shift;
    my $type = shift;

    printlogln($g_prefix,$g_logfile,$g_log_prefix,lookupMsg('exit', $type));

    if ($g_log_to_db) {    
        eval {
           logDB("dbname=$g_logdb_name;host=$g_logdb_host;port=$g_logdb_port", $g_logdb_user, $g_logdb_pass, $exit_code, $g_reason, $g_prefix, $g_logfile, $g_log_prefix, $g_clname, $g_script);
        };
    }

    if ($g_pid_written) {
        removePID($g_prefix, $g_logfile, $g_log_prefix, $g_pidfile);
    }

    exit($exit_code);
}

sub sigExit {
    cleanExit(100,'signal');    
}

sub checkNodes {
    my $clname = shift;
    my $dbuser = shift;
    my $dbpass = shift;
    my $from = shift;
    my $to = shift;
    my $prefix = shift;
    my $logfile = shift;
    my $log_prefix = shift;

    my $dsn;
    my $dbh;
    my $sth;
    my $query;
    my $result_count = 0;
    my $critical_count = 0;

    my @subsets;
    my @origsets;

    undef @g_failed;
    undef @g_unresponsive;
    undef %g_backups;

    foreach (@g_cluster) {
        if ($_->[0] == $from) {
            @origsets = split(',', $_->[3]); 
            last;
        }
    }

    foreach (@g_cluster) {
        if ($g_debug) {
            printlogln($prefix,$logfile,$log_prefix,lookupMsg('dbg_cluster', $_->[0],($_->[1] // "<NONE>"),$_->[2],($_->[3] // "<NONE>"),$_->[4],($_->[5] // "<NONE>") . "(" . ($_->[8] // "<NONE>") . ")",$_->[6],($_->[7] // "<NONE>"),($_->[9] // "<NONE>") . " (" . ($_->[10] // "<NONE>") . ")"));
        }
            
        $dsn = "DBI:Pg:$_->[2];";
        eval {
            $dbh = DBI->connect($dsn, $dbuser, $dbpass, {RaiseError => 1});
            $query = "SELECT count(*) FROM pg_namespace WHERE nspname = ?";
            $sth = $dbh->prepare($query);
            $sth->bind_param(1, "_" . $clname);
            $sth->execute();
        
            $result_count = $result_count+$sth->rows;        

            $sth->finish;
            $dbh->disconnect();

        };
        if ($@) {
            # Critical count will be -1 if the new origin is down, -2 if the old origin is down or positive if subscribers to sets on old origin are down.
            printlogln($prefix,$logfile,$log_prefix,lookupMsg('wrn_node_unavailable', $_->[0]));
            if ($g_debug) {
                printlogln($prefix,$logfile,$log_prefix,lookupMsg('dbg_generic', $@));
            }
            if ($_->[0] == $to) {
                $critical_count = -1;        
            }
            elsif ($_->[0] == $from) {
                $critical_count = -2;        
            }
            else {
                foreach my $subprov (split(';', $_->[5])) {
                    my ($node, $setlist) = (split('->', $subprov)) ;
                    $node =~ s/n//g;
                    $setlist =~ s/(\)|\(|s)//g;
                    @subsets = (split(',', $setlist));

                    if (($critical_count >= 0) && (checkSubscribesAnySets(\@origsets, \@subsets))) {
                        $critical_count++;    
                    }
                }
            }
            # Only push nodes with active subscribers to sets into failed list unless explicitly told to
            if (($g_fail_subonly) || (defined($_->[9]))) {
                push(@g_failed, \@$_);
                $g_backups{$_->[0]} = $to;
            }
            push(@g_unresponsive, \@$_);
        }    
        
    }
    return ($result_count, $critical_count);
}

sub loadCluster {
    my $dbconninfo = shift;
    my $clname = shift;
    my $dbuser = shift;
    my $dbpass = shift;
    my $prefix = shift;
    my $logfile = shift;
    my $log_prefix = shift;

    my $dsn;
    my $dbh;
    my $sth;
    my $query;
    my $version;
    my $qw_clname;
    undef @g_cluster;

    if ($g_debug) {
        printlogln($prefix,$logfile,$log_prefix,lookupMsg('dbg_cluster_load', $dbconninfo));
    }

    $dsn = "DBI:Pg:$dbconninfo;";
    eval {
        $dbh = DBI->connect($dsn, $dbuser, $dbpass, {RaiseError => 1});
        $qw_clname = $dbh->quote_identifier("_" . $clname);

        $query = "SELECT $qw_clname.getModuleVersion()";
        $sth = $dbh->prepare($query);
        $sth->execute();
        ($version) = $sth->fetchrow; 
        $sth->finish;

        $query = "WITH z AS (
                SELECT a.no_id, b.sub_provider AS no_prov,
                    COALESCE(c.pa_conninfo,(SELECT pa_conninfo FROM $qw_clname.sl_path WHERE pa_server = $qw_clname.getlocalnodeid(?) LIMIT 1)) AS no_conninfo,
                    array_to_string(array(SELECT set_id FROM $qw_clname.sl_set WHERE set_origin = a.no_id ORDER BY set_id),',') AS origin_sets,
                    string_agg(CASE WHEN b.sub_receiver = a.no_id AND b.sub_forward AND b.sub_active THEN b.sub_set::text END, ',' ORDER BY b.sub_set) AS prov_sets,
                    coalesce(trim(regexp_replace(substring(a.no_comment from E'\\\\((.+)\\\\)'), '[^0-9A-Za-z]','_','g')), 'node' || a.no_id) AS no_name,
                    'n' || b.sub_provider || '->(' || string_agg(CASE WHEN b.sub_receiver = a.no_id THEN 's' || b.sub_set END,',' ORDER BY b.sub_set,',') || ')' AS sub_tree,
                    coalesce(trim(regexp_replace(substring(d.no_comment from E'\\\\((.+)\\\\)'), '[^0-9A-Za-z]','_','g')), 'node' || b.sub_provider, '')
                    || '->(' || string_agg(CASE WHEN b.sub_receiver = a.no_id THEN coalesce(trim(regexp_replace(e.set_comment, '[^0-9A-Za-z]', '_', 'g')), 'set' || b.sub_set) END,',' ORDER BY b.sub_set) || ')' AS sub_tree_name,
                    CASE " . ((substr($version,0,3) >= 2.2) ? "WHEN a.no_failed THEN 'FAILED' " : "") . "WHEN a.no_active THEN 'ACTIVE' ELSE 'INACTIVE' END AS no_status,
                    array_to_string(array(SELECT DISTINCT sub_set::text FROM $qw_clname.sl_subscribe WHERE sub_provider = a.no_id AND sub_active ORDER BY sub_set),',') AS prov_sets_active,
                    string_agg(CASE WHEN b.sub_receiver = a.no_id THEN b.sub_set::text END,',' ORDER BY b.sub_set,',') AS sub_sets    
                FROM $qw_clname.sl_node a
                LEFT OUTER JOIN $qw_clname.sl_subscribe b ON a.no_id = b.sub_receiver
                LEFT OUTER JOIN $qw_clname.sl_path c ON c.pa_server = a.no_id AND c.pa_client = $qw_clname.getlocalnodeid(?)
                LEFT OUTER JOIN $qw_clname.sl_node d ON b.sub_provider = d.no_id
                LEFT OUTER JOIN $qw_clname.sl_set e ON b.sub_set = e.set_id
                GROUP BY b.sub_provider, a.no_id, a.no_comment, c.pa_conninfo, d.no_comment, a.no_active
                ORDER BY a.no_id
                )
                SELECT no_id,
                    nullif(string_agg(no_prov::text, ',' ORDER BY no_prov),'') AS no_provs,
                    no_conninfo,
                    nullif(string_agg(origin_sets::text, ',' ORDER BY origin_sets),'') AS origin_sets,
                    no_name,
                    nullif(string_agg(sub_tree, ';' ORDER BY sub_tree),'') AS no_sub_tree,
                    no_status,
                    nullif(string_agg(prov_sets::text, ',' ORDER BY prov_sets),'') AS prov_sets,
                    nullif(string_agg(sub_tree_name, ';' ORDER BY sub_tree_name),'') AS no_sub_tree_name,
                    nullif(string_agg(prov_sets_active::text, ',' ORDER BY prov_sets_active),'') AS prov_sets_active,
                    nullif(string_agg(sub_sets::text, ',' ORDER BY sub_sets),'') AS no_subs
                FROM z GROUP BY no_id, no_conninfo, no_name, no_status";
        $sth = $dbh->prepare($query);

        $sth->bind_param(1, "_" . $clname);
        $sth->bind_param(2, "_" . $clname);

        $sth->execute();

        while (my @node = $sth->fetchrow) { 
            push(@g_cluster,  \@node);
        }

        $sth->finish;

        $dbh->disconnect();
    };
    if ($@) {
        if ($g_debug) {
            printlogln($prefix,$logfile,$log_prefix,lookupMsg('dbg_generic', $@));
        }
        die lookupMsg('err_pgsql_connect');
    }
    else {
        #if (substr($version,0,1) < 2) {
        #    printlogln($prefix,$logfile,$log_prefix,lookupMsg('wrn_not_tested', $version));
        #}
        if (($g_use_try_blocks) && ($g_lockset_method eq 'multiple') && (substr($version,0,3) <= 9.9)) {
            # It's currently not possible to lock multiple sets at a time within a try block (v2.2.2), leave the logic in and set a high version number for now.
            printlogln($prefix,$logfile,$log_prefix, lookupMsg('note_multiple_try', $version));
            $g_lockset_method = 'single';
        }
        if (substr($version,0,3) >= 2.2) {
            $g_failover_method = 'new';
            $g_resubscribe_method = 'resubscribe';
        }
        else {
            unless ($g_silence_notice) {
                if ((substr($version,0,3) >= 2.0) && (substr($version,0,3) < 2.2)) {
                    printlogln($prefix,$logfile,$log_prefix,lookupMsg('wrn_failover_issues', $version));
                }
                printlogln($prefix,$logfile,$log_prefix,lookupMsg('note_autofail_fwd_only'));
                $g_silence_notice = true;
            }
            if ($g_fail_subonly) {
                printlogln($prefix,$logfile,$log_prefix,lookupMsg('note_fail_sub_only'));
                $g_fail_subonly = false;
            }
        }
        
    }

    return (scalar(@g_cluster), $version);
}

sub loadSets {
    my $dbconninfo = shift;
    my $clname = shift;
    my $nodenumber = shift;
    my $dbuser = shift;
    my $dbpass = shift;
    my $prefix = shift;
    my $logfile = shift;
    my $log_prefix = shift;
    
    my $dsn;
    my $dbh;
    my $sth;
    my $query;
    my $qw_clname;

    @g_sets = ();
    
    $dsn = "DBI:Pg:$dbconninfo;";
    eval {
        $dbh = DBI->connect($dsn, $dbuser, $dbpass, {RaiseError => 1});
        $qw_clname = $dbh->quote_identifier("_" . $clname);
        $query = "SELECT set_id, trim(regexp_replace(set_comment,'[^0-9,A-Z,a-z]','_','g')) FROM $qw_clname.sl_set WHERE set_origin = ? ORDER BY set_id;";

        $sth = $dbh->prepare($query);
        $sth->bind_param(1, $nodenumber);

        $sth->execute();

        while (my @set = $sth->fetchrow) { 
            push(@g_sets,  \@set);
        }

        $sth->finish;
        $dbh->disconnect();
    };
    if ($@) {
        if ($g_debug) {
            printlogln($prefix,$logfile,$log_prefix,lookupMsg('dbg_generic', $@));
        }
        die lookupMsg('err_pgsql_connect');
    }

    return scalar(@g_sets);
}

sub loadLag {
    my $dbconninfo = shift;
    my $clname = shift;
    my $dbuser = shift;
    my $dbpass = shift;
    my $prefix = shift;
    my $logfile = shift;
    my $log_prefix = shift;

    my $dsn;
    my $dbh;
    my $sth;
    my $query;
    my $qw_clname;

    @g_lags = ();

    $dsn = "DBI:Pg:$dbconninfo;";
    eval {
        $dbh = DBI->connect($dsn, $dbuser, $dbpass, {RaiseError => 1});
        $qw_clname = $dbh->quote_identifier("_" . $clname);
        $query = "SELECT a.st_origin || ' (' || coalesce(trim(regexp_replace(substring(b.no_comment from E'\\\\((.+)\\\\)'), '[^0-9A-Za-z]','_', 'g')), 'node' || b.no_id) || ')<->'
                || a.st_received || ' (' || coalesce(trim(regexp_replace(substring(c.no_comment from E'\\\\((.+)\\\\)'), '[^0-9A-Za-z]','_', 'g')), 'node' || c.no_id) || ') Events: '
                || a.st_lag_num_events || ' Time: ' || a.st_lag_time 
            FROM $qw_clname.sl_status a
            INNER JOIN $qw_clname.sl_node b on a.st_origin = b.no_id
            INNER JOIN $qw_clname.sl_node c on a.st_received = c.no_id";

        $sth = $dbh->prepare($query);
        $sth->execute();

        while (my $lag = $sth->fetchrow) { 
            push(@g_lags,  $lag);
        }

        $sth->finish;
        $dbh->disconnect();
    };
    if ($@) {
        if ($g_debug) {
            printlogln($prefix,$logfile,$log_prefix,lookupMsg('dbg_generic', $@));
        }
        die lookupMsg('err_pgsql_connect');
    }

    return scalar(@g_lags);
}

sub chooseNode {
    my $type = shift;
    my $prefix = shift;
    my $logfile = shift;
    my $log_prefix = shift;
    my $last_choice = shift;
    my $line;
    my $choice;
    my %options;
    my $ok;
    my @sets_from;
    my @sets_to;
    my $found = false;

    $line = sprintf "%-4s %-14s %-10s %-24s %-s\n", lookupMsg('interactive_head_id'), lookupMsg('interactive_head_name'), lookupMsg('interactive_head_status'), lookupMsg('interactive_head_providers'), lookupMsg('interactive_head_config');
    printlog($prefix,$logfile,$log_prefix,"$line");
    $line = sprintf "%-4s %-14s %-10s %-24s %-s\n", "="x(length(lookupMsg('interactive_head_id'))), "="x(length(lookupMsg('interactive_head_name'))), "="x(length(lookupMsg('interactive_head_status'))), "="x(length(lookupMsg('interactive_head_providers'))), "="x(length(lookupMsg('interactive_head_config')));
    printlog($prefix,$logfile,$log_prefix,"$line");

    foreach (@g_cluster) {
        $line = sprintf "%-4s %-14s %-10s %-24s %-s\n", $_->[0], $_->[4], $_->[6], ($_->[1] // "<NONE>"), (lookupMsg('interactive_detail_1') . ($_->[3] // "<NONE>"));
        printlog($prefix,$logfile,$log_prefix,"$line");
        $line = sprintf "%-55s %-s\n", " ", (lookupMsg('interactive_detail_2') . ($_->[7] // "<NONE>"));
        printlog($prefix,$logfile,$log_prefix,"$line");
        $line = sprintf "%-55s %-s\n", " ", (lookupMsg('interactive_detail_3') . ($_->[5] // "<NONE>"));
        printlogln($prefix,$logfile,$log_prefix,"$line");
        $options{$_->[0]} = {name => $_->[4], sets => ($_->[3] // ""), status => $_->[6], provider => $_->[7]};
    }
    if ($type !~ m/info/i) {
        printlog($prefix,$logfile,$log_prefix,lookupMsg('interactive_choose_node', $type));
        $choice = <>;
        chomp($choice);
    
        if(exists($options{$choice})) {
            if ($options{$choice}->{status} ne "ACTIVE") {
                printlogln($prefix,$logfile,$log_prefix,lookupMsg('err_inactive', $choice, lc($options{$choice}->{status})));
                $choice = -1;
            }
            elsif (($type =~ m/from/i) && (length(trim($options{$choice}->{sets})) <= 0)) {
                printlogln($prefix,$logfile,$log_prefix,lookupMsg('err_not_origin', $choice));
                $choice = -1;
            }    
            elsif ($type =~ m/to/i) {
                if (length(trim($options{$choice}->{provider})) <= 0) {
                    printlogln($prefix,$logfile,$log_prefix,lookupMsg('err_not_provider', $choice));
                    $choice = -1;
                }
                else {
                    foreach my $old_origin (@g_cluster) {
                        if ($old_origin->[0] == $last_choice) {
                            @sets_from = split(',', $old_origin->[3]);
                            @sets_to =  split(',', $options{$choice}->{provider});
                            if (checkProvidesAllSets(\@sets_from, \@sets_to)) {
                                $found = true;
                            }
                            else {
                                printlogln($prefix,$logfile,$log_prefix,lookupMsg('err_not_provider_sets',$choice,$old_origin->[3],$options{$choice}->{providers}));
                                $choice = -1;
                            }
                            last;
                        }
                    }
                    unless ($found) {
                        printlogln($prefix,$logfile,$log_prefix,lookupMsg('err_no_configuration', $last_choice));
                        $choice = -1;
                    } 
                }
            }    
            else {
                printlog($prefix,$logfile,$log_prefix,lookupMsg('interactive_confirm',$type,$choice,$options{$choice}->{name}));
                $ok = <>;
                chomp($ok);    
                if ($ok !~ /^[Y|O]$/i) {
                    printlogln($prefix,$logfile,$log_prefix,lookupMsg('exit_noaction'));
                    $choice = 0;
                }
            }
        }
        elsif (!length($choice)) {
            printlogln($prefix,$logfile,$log_prefix,lookupMsg('err_must_enter_node_id'));
            $choice = -1;
        }
        else {
            printlogln($prefix,$logfile,$log_prefix,lookupMsg('err_not_a_node_id', $choice));
            $choice = -1;
        }
    }

    return $choice;
}

sub writePreamble {
    my $filename = shift;
    my $dbconninfo = shift;
    my $clname = shift;
    my $dbuser = shift;
    my $dbpass = shift;
    my $sets = shift;
    my $aliases = shift;
    my $prefix = shift;
    my $logfile = shift;
    my $log_prefix = shift;
    my $comment_all_failed = shift;
    my $set_count;
    my $line_prefix;
    my $success = false;

    my ($year, $month, $day, $hour, $min, $sec) = (localtime(time))[5,4,3,2,1,0];
    my $date = sprintf ("%02d:%02d:%02d on %02d/%02d/%04d", $hour, $min, $sec, $day, $month+1, $year+1900);

    if (open(SLONFILE, ">", $filename)) {    
        print SLONFILE ("# Script autogenerated on $date\n\n");
        print SLONFILE ("######\n# Preamble (cluster structure)\n######\n\n# Cluster name\n");
        if ($aliases) {
            print SLONFILE ("DEFINE slony_cluster_name $clname;\n");
            print SLONFILE ("CLUSTER NAME = \@slony_cluster_name;\n\n");
        }
        else {
            print SLONFILE ("CLUSTER NAME = $clname;\n\n");
        }
        foreach (@g_cluster) {
            $line_prefix = '';
            if (($comment_all_failed) && (exists $g_backups{$_->[0]})) {
                $line_prefix = "# (Node $_->[0] unavailable) ";
            }
            elsif (!$g_fail_subonly) {
                foreach my $unresponsive (@g_unresponsive) {
                    if (($_->[0] == $unresponsive->[0]) && !defined($_->[9]) && ($g_failover_method eq 'new')) {
                        $line_prefix = "# (Node $_->[0] unavailable subscriber only) ";
                    }
                }
            }
            print SLONFILE ("# Preamble for node $_->[0] named $_->[4]\n");
            if ($aliases) {
                print SLONFILE ($line_prefix . "DEFINE $_->[4] $_->[0];\n");
                print SLONFILE ($line_prefix . "DEFINE $_->[4]_conninfo '$_->[2]';\n");
                print SLONFILE ($line_prefix . "NODE \@$_->[4] ADMIN CONNINFO = \@$_->[4]_conninfo;\n\n");
            }
            else {
                print SLONFILE ($line_prefix . "NODE $_->[0] ADMIN CONNINFO = '$_->[2]';\n\n");
            }
            if (($aliases) && ($sets)) {
                $set_count = loadSets($dbconninfo, $clname, $_->[0], $dbuser, $dbpass, $prefix, $logfile, $log_prefix);
                if ($set_count > 0) {
                    print SLONFILE ("# Sets provided (currently) by node $_->[0]\n");
                    foreach my $set (@g_sets) {
                        print SLONFILE ($line_prefix . "DEFINE $set->[1] $set->[0];\n");
                    }
                    print SLONFILE ("\n");
                }
            }
        }    
        $success = true;
    }
    else {
        printlogln($prefix,$logfile,$log_prefix,lookupMsg('err_write_fail', "script", $filename));
        $success = false; 
    }
    return $success;
}

sub writeMoveSet {
    my $prefix = shift;
    my $dbconninfo = shift;
    my $clname = shift;
    my $dbuser = shift;
    my $dbpass = shift;
    my $from = shift;
    my $to = shift;
    my $subs = shift;
    my $aliases = shift;
    my $logfile = shift;
    my $log_prefix = shift;
    my $from_name;
    my $to_name;
    my $set_count;
    my $line_prefix;
    my $try_prefix = "";
    my ($year, $month, $day, $hour, $min, $sec) = (localtime(time))[5,4,3,2,1,0];
    my $filetime = sprintf ("%02d_%02d_%04d_%02d:%02d:%02d", $day, $month+1, $year+1900, $hour, $min, $sec);
    my $filename = $prefix . "/" . $clname . "-move_sets_from_" . $from . "_to_" . $to . "_on_" . $filetime . ".scr";

    if ($g_use_try_blocks) {
        $try_prefix = "\t";
    }
 
    my @subprov_name;
    my $subprov_idx;
    my $subprov;
    my ($node, $setlist);
    my ($node_name, $setlist_name);
    my @subsets;

    unless (writePreamble($filename, $dbconninfo, $clname, $dbuser, $dbpass, true, $aliases, $prefix, $logfile, $log_prefix, true)) {
        printlogln($prefix,$logfile,$log_prefix,lookupMsg('err_incomplete_preamble'));
    }

    foreach (@g_cluster) {
        if ($_->[0] == $from) {
            $from_name = $_->[4];
        }
        elsif ($_->[0] == $to) {
            $to_name = $_->[4];
        }
    }

    if (open(SLONFILE, ">>", $filename)) {

        print SLONFILE ("######\n# Actions (changes to cluster structure)\n######\n");
        
        $set_count = loadSets($dbconninfo, $clname, $from, $dbuser, $dbpass, $prefix, $logfile, $log_prefix);
        if ($set_count > 0) {

            if ($g_lockset_method ne "single") {
                if ($g_use_try_blocks) {
                    print SLONFILE ("TRY {\n");                
                }
                foreach (@g_sets) {
                    if ($aliases) {    
                        print SLONFILE ($try_prefix . "ECHO 'Locking set $_->[1] ($_->[0])';\n");
                        print SLONFILE ($try_prefix . "LOCK SET ( ID = \@$_->[1], ORIGIN = \@$from_name);\n");
                    }
                    else {
                        print SLONFILE ($try_prefix . "ECHO 'Locking set $_->[0]';\n");
                        print SLONFILE ($try_prefix . "LOCK SET ( ID = $_->[0], ORIGIN = $from);\n");
                    }
                
                }
                print SLONFILE ("\n");
                foreach (@g_sets) {
                    if ($aliases) {    
                        print SLONFILE ($try_prefix . "ECHO 'Moving set $_->[1] ($_->[0])';\n");
                        print SLONFILE ($try_prefix . "MOVE SET ( ID = \@$_->[1], OLD ORIGIN = \@$from_name, NEW ORIGIN = \@$to_name);\n");
                    }
                    else {
                        print SLONFILE ($try_prefix . "ECHO 'Moving set $_->[0]';\n");
                        print SLONFILE ($try_prefix . "MOVE SET ( ID = $_->[0], OLD ORIGIN = $from, NEW ORIGIN = $to);\n");
                    }
                
                }                
                if ($g_use_try_blocks) {
                    print SLONFILE ("}\nON ERROR {\n");
                    foreach (@g_sets) {
                        if ($aliases) {    
                            print SLONFILE ($try_prefix . "ECHO 'Unlocking set $_->[1] ($_->[0])';\n");
                            print SLONFILE ($try_prefix . "UNLOCK SET ( ID = \@$_->[1], ORIGIN = \@$from_name);\n");
                        }
                        else {
                            print SLONFILE ($try_prefix . "ECHO 'Unlocking set $_->[0]';\n");
                            print SLONFILE ($try_prefix . "UNLOCK SET ( ID = $_->[0], ORIGIN = $from);\n");
                        }
                    }
                    print SLONFILE ("\tEXIT 1;\n}\nON SUCCESS {\n");
                }
                if ($aliases) {    
                    print SLONFILE ($try_prefix . "WAIT FOR EVENT (ORIGIN = \@$from_name, CONFIRMED = ALL, WAIT ON = \@$from_name, TIMEOUT = 0);\n");
                }
                else {
                    print SLONFILE ($try_prefix . "WAIT FOR EVENT (ORIGIN = $from, CONFIRMED = ALL, WAIT ON = $from, TIMEOUT = 0);\n");
                }
                if ($g_use_try_blocks) {
                    print SLONFILE ("}\n");
                }
            }
            foreach (@g_sets) {
                if ($g_lockset_method eq "single") {
                    if ($aliases) {    
                        print SLONFILE ("\nECHO 'Moving set $_->[1] ($_->[0])';\n");
                        if ($g_use_try_blocks) {
                            print SLONFILE ("TRY {\n");
                        }
                        print SLONFILE ($try_prefix . "LOCK SET ( ID = \@$_->[1], ORIGIN = \@$from_name);\n");
                        print SLONFILE ($try_prefix . "MOVE SET ( ID = \@$_->[1], OLD ORIGIN = \@$from_name, NEW ORIGIN = \@$to_name);\n");
                        if ($g_use_try_blocks) {
                            print SLONFILE ("}\nON ERROR {\n" . $try_prefix . "UNLOCK SET ( ID = \@$_->[1], ORIGIN = \@$from_name);\n" . $try_prefix . "EXIT 1;\n}\n");
                        }
                        print SLONFILE ("WAIT FOR EVENT (ORIGIN = \@$from_name, CONFIRMED = ALL, WAIT ON = \@$from_name, TIMEOUT = 0);\n");
                    }
                    else {
                        print SLONFILE ("\nECHO 'Moving set $_->[0]';\n");
                        if ($g_use_try_blocks) {
                            print SLONFILE ("TRY {\n");
                        }
                        print SLONFILE ($try_prefix . "LOCK SET ( ID = $_->[0], ORIGIN = $from);\n");
                        print SLONFILE ($try_prefix . "MOVE SET ( ID = $_->[0], OLD ORIGIN = $from, NEW ORIGIN = $to);\n");
                        if ($g_use_try_blocks) {
                            print SLONFILE ("}\nON ERROR {\n" . $try_prefix . "UNLOCK SET ( ID = $_->[0], ORIGIN = $from);\n" . $try_prefix . "EXIT 1;\n}\n");
                        }
                        print SLONFILE ("WAIT FOR EVENT (ORIGIN = $from, CONFIRMED = ALL, WAIT ON = $from, TIMEOUT = 0);\n");
                    }
                }    
                if (($subs) && ($g_resubscribe_method eq 'subscribe')) { 
    
                    foreach my $other_subs (@g_cluster) {
                        if (($other_subs->[6] eq "ACTIVE") && ($other_subs->[0] != $from) && ($other_subs->[0] != $to)) {

                            if (exists $g_backups{$other_subs->[0]}) {
                                $line_prefix = "# (Node $other_subs->[0] unavailable) ";
                            }
                            else {
                                $line_prefix = '';
                            }

                            # mess here needs cleaning up
                            @subprov_name = (split(';', $other_subs->[8]));
                            $subprov_idx = 0;
                            foreach $subprov (split(';', $other_subs->[5])) {
                                ($node, $setlist) = (split('->', $subprov)) ;
                                ($node_name, $setlist_name) = (split('->', $subprov_name[$subprov_idx])) ;
                                $subprov_idx++;
                                $node =~ s/n//g;
                                $setlist =~ s/(\)|\(|s)//g;
                                @subsets = (split(',', $setlist)) ;

                                if ($g_debug) {
                                    printlogln($prefix,$logfile,$log_prefix,lookupMsg('dbg_resubscribe', $_->[1], $_->[0], $other_subs->[0], $other_subs->[4], $setlist, $setlist_name, $node, $node_name));
                                }    

                                if ($_->[0] ~~ @subsets) {
                                    if ($node == $from) {
                                        if ($aliases) {
                                            print SLONFILE ($line_prefix . 
                                                "ECHO 'Issuing subscribe for set $_->[1] ($_->[0]) provider $to_name ($to) -> " .
                                                "receiver $other_subs->[4] ($other_subs->[0])';\n");
                                                   print SLONFILE ($line_prefix . 
                                                "SUBSCRIBE SET ( ID = \@$_->[1], PROVIDER = \@$to_name, " .
                                                "RECEIVER = \@$other_subs->[4], FORWARD = YES);\n");
                                        }
                                        else {
                                            print SLONFILE ($line_prefix . 
                                                "ECHO 'Issuing subscribe for set $_->[1] ($_->[0]) provider $to -> " .
                                                "receiver $other_subs->[0]';\n");
                                            print SLONFILE ($line_prefix . "SUBSCRIBE SET ( ID = $_->[0], PROVIDER = $to, " .
                                                "RECEIVER = $other_subs->[0], FORWARD = YES);\n");
                                        }
                                    }
                                    else {
                                        if ($aliases) {
                                            print SLONFILE ($line_prefix . 
                                                "ECHO 'Issuing subscribe for set $_->[1] ($_->[0]) provider $node_name ($node) -> " . 
                                                "receiver $other_subs->[4] ($other_subs->[0])';\n");
                                            print SLONFILE ($line_prefix . "SUBSCRIBE SET ( ID = \@$_->[1], PROVIDER = \@$node_name, " .
                                                "RECEIVER = \@$other_subs->[4], FORWARD = YES);\n");
                                            }
                                            else {
                                                print SLONFILE ($line_prefix . 
                                                "ECHO 'Issuing subscribe for set $_->[1] ($_->[0]) provider $node -> " .
                                                "receiver $other_subs->[0]';\n");
                                                print SLONFILE ($line_prefix . "SUBSCRIBE SET ( ID = $_->[0], PROVIDER = $node, " .
                                                "RECEIVER = $other_subs->[0], FORWARD = YES);\n");
                                        }
                                    }
                                }
                            }
                        }
                    }
                }    
            }

            if (($subs) && ($g_resubscribe_method eq 'resubscribe')) { 

                foreach my $other_subs (@g_cluster) {
                    if (($other_subs->[6] eq "ACTIVE") && ($other_subs->[0] != $from) && ($other_subs->[0] != $to)) {
                        if (exists $g_backups{$other_subs->[0]}) {
                            $line_prefix = "# (Node $other_subs->[0] unavailable) ";
                        }
                        else {
                            $line_prefix = '';
                        }

                        @subprov_name = (split(';', $other_subs->[8]));
                        $subprov_idx = 0;
                        foreach $subprov (split(';', $other_subs->[5])) {
                            ($node, $setlist) = (split('->', $subprov)) ;
                            ($node_name, $setlist_name) = (split('->', $subprov_name[$subprov_idx])) ;
                            $subprov_idx++;
                            $node =~ s/n//g;
    
                            print SLONFILE ("\n");
                            if ($node == $from) {
                                if ($aliases) {
                                    print SLONFILE ($line_prefix .
                                        "ECHO 'Issuing resubscribe for provider $to_name ($to) -> receiver $other_subs->[4] ($other_subs->[0])';\n");
                                    print SLONFILE ($line_prefix .
                                        "RESUBSCRIBE NODE ( ORIGIN = \@$to_name, PROVIDER = \@$to_name, RECEIVER = \@$other_subs->[4]);\n");
                                 }
                                 else {
                                     print SLONFILE ($line_prefix .
                                        "ECHO 'Issuing resubscribe for provider $to -> receiver $other_subs->[0]';\n");
                                    print SLONFILE ($line_prefix . 
                                        "RESUBSCRIBE NODE ( ORIGIN = $to, PROVIDER = $to, RECEIVER = $other_subs->[0] );\n");
                                 }            
                            }
                            else {
                                if ($aliases) {
                                    print SLONFILE ($line_prefix .
                                        "ECHO 'Issuing resubscribe for provider $node_name ($node) -> receiver $other_subs->[4] ($other_subs->[0])';\n");
                                    print SLONFILE ($line_prefix . 
                                        "RESUBSCRIBE NODE ( ORIGIN = \@$to_name, PROVIDER = \@$node_name, RECEIVER = \@$other_subs->[4]);\n");
                                }
                                else {
                                    print SLONFILE ($line_prefix .
                                        "ECHO 'Issuing resubscribe for provider $node -> receiver $other_subs->[0]';\n");
                                    print SLONFILE ($line_prefix .
                                        "RESUBSCRIBE NODE ( ORIGIN = $to, PROVIDER = $node, RECEIVER = $other_subs->[0]);\n");
                                }
                            }
                        }
                    }
                }
            }

        }

        if ($aliases) {    
            print SLONFILE ("\nECHO 'All sets originating from $from_name (id $from) have been moved to $to_name (id $to), ensure you modify any existing slonik scripts to reflect the new origin';\n");
        }
        else {
            print SLONFILE ("\nECHO 'All sets originating from node $from have been moved to node $to, ensure you modify the any existing slonik scripts to reflect the new origin';\n");
        }
        close (SLONFILE);
    }
    else {
        printlogln($prefix,$logfile,$log_prefix,lookupMsg('err_write_fail', "script", $filename));
    }
    return $filename;
}

sub writeFailover {
    my $prefix = shift;
    my $dbconninfo = shift;
    my $clname = shift;
    my $dbuser = shift;
    my $dbpass = shift;
    my $from = shift;
    my $to = shift;
    my $subs = shift;
    my $aliases = shift;
    my $logfile = shift;
    my $log_prefix = shift;
    my $filename;
    my $written;
    my $event_node;
    my ($year, $month, $day, $hour, $min, $sec) = (localtime(time))[5,4,3,2,1,0];
    my $filetime = sprintf ("%02d_%02d_%04d_%02d:%02d:%02d", $day, $month+1, $year+1900, $hour, $min, $sec);
    my $sets = false;

    my $subprov_idx;
    my @subprov_name;
    my ($node, $setlist);
    my ($node_name, $setlist_name);
    my @subsets;
    my @subsets_name;
    my $set_idx;
    my @dropped;

    if (defined($from) && defined($to)) {
        $filename = $prefix . "/" . $clname . "-failover_from_" . $from . "_to_" . $to . "_on_" . $filetime . ".scr";
    }
    else {
        $filename = $prefix . "/" . $clname . "-autofailover_on_" . $filetime . ".scr";
    }

    if ($g_failover_method ne 'new') {
        # For pre 2.2 failover with multiple nodes, we attempt to resubscribe sets and drop other failed providers;
        # This will never work as well as 2.2+ failover behaviour (infact failover may not work as all in 2.0/2.1 with multiple failed nodes)
        # We also need to define the sets in the preamble for this.
        $sets = true;
    }

    unless (writePreamble($filename, $dbconninfo, $clname, $dbuser, $dbpass, $sets, $aliases, $prefix, $logfile, $log_prefix, false)) {
        printlogln($prefix,$logfile,$log_prefix,lookupMsg('err_incomplete_preamble'));
    }

    if (open(SLONFILE, ">>", $filename)) {

        print SLONFILE ("######\n# Actions (changes to cluster structure)\n######\n\n");
        if ($g_debug) {
            printlogln($prefix,$logfile,$log_prefix,lookupMsg('dbg_failover_method',$g_failover_method));
        }

        # If we are on pre 2.2 we need to drop failed subscriber nodes first regardless
        if ($g_failover_method ne 'new') {
            foreach (@g_failed) {
                if (!defined($_->[3])) {
                    foreach my $backup (@g_cluster) {
                        if ($backup->[0] == $g_backups{$_->[0]}) {  # this backup node candidate is in the list of suitable nodes for {failed node}
                            foreach my $subscriber (@g_cluster) {
                                if (defined($subscriber->[1]) && $subscriber->[1] == $_->[0] && $subscriber->[0] != $backup->[0]) {
                                    # mess here needs cleaning up
                                    @subprov_name = (split(';', $subscriber->[8]));
                                    $subprov_idx = 0;
                                    foreach my $subprov (split(';', $subscriber->[5])) {
                                        ($node, $setlist) = (split('->', $subprov)) ;
                                        ($node_name, $setlist_name) = (split('->', $subprov_name[$subprov_idx])) ;
                                        $subprov_idx++;
                                        $node =~ s/n//g;
    
                                        if ($node == $_->[0]) {
                                            if ($aliases) {
                                                print SLONFILE ("ECHO 'Resubscribing all sets on receiver $subscriber->[4] provided by other failed node $_->[4] to backup node $backup->[4]';\n");
                                            }
                                            else {
                                                print SLONFILE ("ECHO 'Resubscribing all sets on receiver $subscriber->[0]  provided by other failed node $_->[0] to backup node $backup->[0]';\n");
                                            }
                                            $setlist =~ s/(\)|\(|s)//g;
                                            @subsets = (split(',', $setlist));
                                            $setlist_name =~ s/(\)|\()//g;
                                            @subsets_name = (split(',', $setlist_name));
                                        
                                            $set_idx = 0;
                                            foreach my $subset (@subsets) {
                                                if ($aliases) {
                                                    print SLONFILE ("SUBSCRIBE SET (ID = \@$subsets_name[$set_idx], PROVIDER = \@$backup->[4], RECEIVER = \@$subscriber->[4], FORWARD = YES);\n");
                                                    print SLONFILE ("WAIT FOR EVENT (ORIGIN = \@$backup->[4], CONFIRMED = \@$subscriber->[4], WAIT ON = \@$backup->[4]);\n");
                                                }
                                                else {
                                                    print SLONFILE ("SUBSCRIBE SET (ID = $subset, PROVIDER = $backup->[0], RECEIVER = $subscriber->[0], FORWARD = YES);\n");
                                                    print SLONFILE ("WAIT FOR EVENT (ORIGIN = $backup->[0], CONFIRMED = $subscriber->[0], WAIT ON = $backup->[0]);\n");
                                                }
                                                $set_idx++;
                                            }
                                            print SLONFILE ("\n");
                                        }
                                    }
    
                                    if ($aliases) {
                                        print SLONFILE ("ECHO 'Dropping other failed node $_->[4] ($_->[0])';\n");
                                         print SLONFILE ("DROP NODE (ID = \@$_->[4], EVENT NODE = \@$backup->[4]);\n\n");
                                    }
                                    else {
                                        print SLONFILE ("ECHO 'Dropping other failed node $_->[0]';\n");
                                        print SLONFILE ("DROP NODE (ID = $_->[0], EVENT NODE = $backup->[0]);\n\n");
                                    }   
                                    push(@dropped, $_->[0]);
                                }
                                else {
                                    # The node is failed, but there are no downstream subscribers
                                }
                            }
                            last;
                        }
                    }
                }
            }
        }

        foreach (@g_failed) {
            if (($g_failover_method eq 'new') || defined($_->[3])) {
                foreach my $backup (@g_cluster) {
                    if ($backup->[0] == $g_backups{$_->[0]}) {
                        ## Here we have both details of the backup node and the failed node
                        if ($aliases) {
                            print SLONFILE ("ECHO 'Failing over slony cluster from $_->[4] (id $_->[0]) to $backup->[4] (id $backup->[0])';\n");
                        }
                        else {
                            print SLONFILE ("ECHO 'Failing over slony cluster from node $_->[0] to node $backup->[0]';\n");
                        }   
                        last;
                    }
                }
            }
        }

        print SLONFILE ("FAILOVER (\n\t");
        $written = 0;
        foreach (@g_failed) {
            if (($g_failover_method eq 'new') || defined($_->[3])) {
                foreach my $backup (@g_cluster) {
                    if ($backup->[0] == $g_backups{$_->[0]}) {
                        ## Here we have both details of the backup node and the failed node
                        if ($g_failover_method eq 'new') {
                            if( $written != 0 ) {
                                print SLONFILE (",\n\t");
                            }
                            print SLONFILE ("NODE = (");
                        }
                        else {
                            if( $written != 0 ) {
                                print SLONFILE ("\n);\nFAILOVER (\n\t");
                            }
                        }
                        if ($aliases) {
                            print SLONFILE ("ID = \@$_->[4], BACKUP NODE = \@$backup->[4]");
                        }
                        else {
                            print SLONFILE ("ID = $_->[0], BACKUP NODE = $backup->[0]");
                        }
                        if ($g_failover_method eq 'new') {
                            print SLONFILE (")");
                        }
                        last;
                    }
                }
                $written++;
            }
        }
        print SLONFILE ("\n);\n\n");

        if ($g_drop_failed) {
            if (($g_failover_method eq 'new')  && (scalar(@g_failed) > 1)) {
                foreach (@g_failed) {
                    if ($aliases) {
                        print SLONFILE ("ECHO 'Dropping failed node $_->[4] ($_->[0])';\n");
                    }
                    else {
                        print SLONFILE ("ECHO 'Dropping failed node $_->[0]';\n");
                    }   
                }

                print SLONFILE ("DROP NODE (ID = '");
                undef $event_node;
            }
            $written = 0;
            foreach (@g_failed) {
                foreach my $backup (@g_cluster) {
                    if ($backup->[0] == $g_backups{$_->[0]}) {
                        if (!defined($event_node)) {
                            if ($aliases) {
                                $event_node = $backup->[4];
                            }
                            else {
                                $event_node = $backup->[0];
                            }
                        } 
                        if (($g_failover_method eq 'new')  && (scalar(@g_failed) > 1)) {
                            if( $written != 0 ) {
                                print SLONFILE (",");
                            }
                            ## Don't bother trying to define array values 
                            #if ($aliases) {
                            #    print SLONFILE "\@$_->[4]";
                            #}
                            #else {
                                print SLONFILE $_->[0];
                            #}
                            $written++;
                        }
                        elsif (($g_failover_method eq 'new') || defined($_->[3]) || !($_->[0] ~~ @dropped)) {
                            if ($aliases) {
                                print SLONFILE ("ECHO 'Dropping failed node $_->[4] ($_->[0])';\n");
                                print SLONFILE ("DROP NODE (ID = \@$_->[4], EVENT NODE = \@$backup->[4]);\n\n");
                            }
                            else {
                                print SLONFILE ("ECHO 'Dropping failed node $_->[0]';\n");
                                print SLONFILE ("DROP NODE (ID = $_->[0], EVENT NODE = $backup->[0]);\n\n");
                            }
                        }
                        last;
                    }
                }   
            }
            if (($g_failover_method eq 'new')  && (scalar(@g_failed) > 1)) {
                if ($aliases) {
                     print SLONFILE ("', EVENT NODE = \@$event_node);\n");
                }
                else {
                     print SLONFILE ("', EVENT NODE = $event_node);\n");
                }
            }
        }

    }
    else {
        printlog($prefix,$logfile,$log_prefix,lookupMsg('err_write_fail', "script", $filename));
    }
    return $filename;

}

sub lookupMsg {
    my $name = shift || '?';
    my $line_call;
    my $text;

    if (exists $message{$g_lang}{$name}) {
        $text = $message{$g_lang}{$name};
    }
    elsif (exists $message{'en'}{$name}) {
        $text = $message{'en'}{$name};
    }
    else {
        $line_call = (caller)[2];
        $text = qq{Failed to lookup text "$name" at line $line_call};
    }

    my $x=1;
    {
        my $val = $_[$x-1];
        $val = '?' if ! defined $val;
        last unless $text =~ s/\$$x/$val/g;
        $x++;
        redo;
    }
    return $text;
}

sub qtrim {
    my $string = shift;
    $string =~ s/^('|")+//;
    $string =~ s/('|")+$//;
    return $string;
}

sub trim($) {
    my $string = shift;
    $string =~ s/^\s+//;
    $string =~ s/\s+$//;
    return $string;
}

sub println {
    print ((@_ ? join($/, @_) : $_), $/);
}

sub printlog {
    my $prefix = shift;
    my $logfile_name = shift;
    my $log_prefix = shift;
    my $message = shift;
    my $logfile;
    my $date;

    print $message;

    if (defined($logfile_name)) {

        # Do we have to do this all the time? Perhaps could check parameters first
        if ($logfile_name =~ /^\//i) {
            $logfile = strftime($logfile_name, localtime);
        }
        else {
            $logfile = "$prefix/" . strftime($logfile_name, localtime);
        }
    
        if ($log_prefix =~ m/(\%[mt])/) {
            my ($year, $month, $day, $hour, $min, $sec) = (localtime(time))[5,4,3,2,1,0];
            my ($h_sec, $h_msec) = gettimeofday;
            $date = sprintf ("%02d-%02d-%04d %02d:%02d:%02d.%03d", $day, $month+1, $year+1900, $hour, $min, $sec, $h_msec/1000);
            $log_prefix =~ s/\%m/$date/g;
    
            $date = sprintf ("%02d-%02d-%04d %02d:%02d:%02d", $day, $month+1, $year+1900, $hour, $min, $sec);
            $log_prefix =~ s/\%t/$date/g;
        }
        if ($log_prefix =~ m/(\%p)/) {
            $log_prefix =~ s/\%p/$g_pid/g;
        }

        if (open(LOGFILE, ">>", $logfile)) {
            print LOGFILE $log_prefix . " " . $message;
            close (LOGFILE);
        }
        else {
            println(lookupMsg('err_write_fail', "logfile", $logfile));
        }
    }
}

sub printlogln {
    printlog ($_[0], $_[1], $_[2], $_[3] . $/);
}

sub logDB {
    my $dbconninfo = shift;
    my $dbuser = shift;
    my $dbpass = shift;
    my $exit_code = shift;
    my $reason = shift;
    my $prefix = shift;
    my $logfile = shift;
    my $log_prefix = shift;
    my $clname = shift;
    my $script = shift;

    my $dsn;
    my $dbh;
    my $sth;
    my $query;

    my $results;
    my $script_data;

    unless($results = (read_file($logfile))) { 
        printlogln($prefix,$logfile,$log_prefix,lookupMsg('err_read_fail', "logfile", $logfile));
    }

    if (defined($script) && (-e $script)) {
        unless ($script_data = (read_file($script))) { 
            printlogln($prefix,$logfile,$log_prefix,lookupMsg('err_read_fail', "script file", $script));
        }
    }
    else {
        $script_data = "No script data was generated.";
        $script = "No script generated.";
    }

    $dsn = "DBI:Pg:$dbconninfo;";
    eval {
        $dbh = DBI->connect($dsn, $dbuser, $dbpass, {RaiseError => 1});
        $query = "INSERT INTO public.failovers (reason, exit_code, results, script, cluster_name)
              VALUES (?, ?, ?, ?, ?)";

        $sth = $dbh->prepare($query);

        $sth->bind_param(1, $reason);
        $sth->bind_param(2, $exit_code);
        $sth->bind_param(3, $results);
        $sth->bind_param(4, $script . ":\n" . $script_data);
        $sth->bind_param(5, $clname);

        $sth->execute();

        $sth->finish;
        $dbh->disconnect();
    };
    if ($@) {
        if ($g_debug) {
            printlogln($prefix,$logfile,$log_prefix,lookupMsg('dbg_generic', $@));
        }
        die lookupMsg('err_pgsql_connect');
    }

    return true;
}

sub getUUID {
    my $date_string = shift;
    my $g_ug  = new Data::UUID;
    my $g_uuid = $g_ug->create_from_name("failover_script", $date_string);
    my $g_uuid_str  = $g_ug->to_string($g_uuid);
    return $g_uuid_str;
}

sub writePID {
    my $prefix = shift;
    my $logfile = shift;
    my $log_prefix = shift;
    my $pidfile_name = shift;
    my $pidfile;
    my $success = true;
 
    if ($pidfile_name =~ /^\//i) {
        $pidfile = $pidfile_name;
    }
    else {
        $pidfile = "$prefix/" . $pidfile_name;
    }
    eval {
        open (PIDFILE, ">", $pidfile);
        print PIDFILE $$;
        close (PIDFILE);
    };
    if ($@) {
        if ($g_debug) {
            printlogln($prefix,$logfile,$log_prefix, lookupMsg('dbg_generic', $!));       
        }
        printlogln($prefix,$logfile,$log_prefix, lookupMsg('err_write_fail', "pid file", $pidfile));
        $success = false;
    }
    return $success;
}

sub removePID {
    my $prefix = shift;
    my $logfile = shift;
    my $log_prefix = shift;
    my $pidfile_name = shift;
    my $pidfile;
    my $success = true;

    if ($pidfile_name =~ /^\//i) {
        $pidfile = $pidfile_name;
    }
    else {
        $pidfile = "$prefix/" . $pidfile_name;
    }
    eval {
        if (-f $pidfile) {
            unlink $pidfile;
        }
        else {
            printlogln($prefix,$logfile,$log_prefix, lookupMsg('dbg_generic', 'PID file never existed to be removed'));
        } 
    };
    if ($@) {
        if ($g_debug) {
            printlogln($prefix,$logfile,$log_prefix, lookupMsg('dbg_generic', $!));
        }
        printlogln($prefix,$logfile,$log_prefix, lookupMsg('err_unlink_fail', "pid file", $pidfile));
        $success = false;
    }
    return $success;
}

sub checkProvidesAllSets { 
    my ($originSets, $providerSets) = @_;
    my %test_hash;

    undef @test_hash{@$originSets};       # add a hash key for each element of @$originSets
    delete @test_hash{@$providerSets};    # remove all keys for elements of @$providerSets

    return !%test_hash;              # return false if any keys are left in the hash
}

sub checkSubscribesAnySets {
    my ($originSets, $subscriberSets) = @_;
    my $before;
    my $after;
    my %test_hash;

    undef @test_hash{@$originSets};       # add a hash key for each element of @$originSets
    $before = scalar(keys %test_hash);
    delete @test_hash{@$subscriberSets};    # remove all keys for elements of @$subscriberSets
    $after = scalar(keys %test_hash);
    return ($before != $after);        # return false if no keys were removed from the hash
}

sub getConfig {
    my $cfgfile = shift;
    my @fields;
    my $success = false;
    my $value;

    if (open(CFGFILE, "<", $cfgfile)) {
        foreach (<CFGFILE>) {
            chomp $_;
            for ($_) {
                s/\r//;
                #s/\#.*//;
                s/#(?=(?:(?:[^']|[^"]*+'){2})*+[^']|[^"]*+\z).*//;
            }
            if (length(trim($_))) {
                @fields = split('=', $_, 2);
                given(lc($fields[0])) {
                    $value = qtrim(trim($fields[1]));
                    when(/\blang\b/i) {
                        $g_lang = $value;
                    }
                    when(/\bslony_database_host\b/i) {
                        $g_dbhost = $value;
                    }
                    when(/\bslony_database_port\b/i) {
                        $g_dbport = checkInteger($value);
                    }
                    when(/\bslony_database_name\b/i) {
                        $g_dbname = $value;
                    }
                    when(/\bslony_database_user\b/i) {
                        $g_dbuser = $value; 
                    }
                    when(/\bslony_database_password\b/i) {
                        $g_dbpass = $value; 
                    }
                    when(/\bslony_cluster_name\b/i) {
                        $g_clname = $value; 
                    }
                    when(/\benable_debugging\b/i) {
                        $g_debug = checkBoolean($value);
                    }
                    when(/\bprefix_directory\b/i) {
                        $g_prefix = $value;
                    }
                    when(/\bseparate_working_directory\b/i) {
                        $g_separate_working = checkBoolean($value);
                    }
                    when(/\bpid_filename\b/i) {
                        $g_pidfile = $value;
                    }
                    when(/\bfailover_offline_subscriber_only\b/i) {
                        $g_fail_subonly = checkBoolean($value);
                    }
                    when(/\bdrop_failed_nodes\b/i) {
                        $g_drop_failed = checkBoolean($value);
                    }
                    when(/\blog_line_prefix\b/i) {
                        $g_log_prefix = $value;
                    }
                    when(/\blog_filename\b/i) {
                        $g_logfile = $value;
                    }
                    when(/\blog_to_postgresql\b/i) {
                        $g_log_to_db = checkBoolean($value);
                    }
                    when(/\blog_database_host\b/i) {
                        $g_logdb_host = $value;
                    }
                    when(/\blog_database_port\b/i) {
                        $g_logdb_port = checkInteger($value);
                    }
                    when(/\blog_database_name\b/i) {
                        $g_logdb_name = $value;
                    }
                    when(/\blog_database_user\b/i) {
                        $g_logdb_user = $value;
                    }
                    when(/\blog_database_password\b/i) {
                        $g_logdb_pass = $value;
                    }
                    when(/\benable_try_blocks\b/i) {
                        $g_use_try_blocks = checkBoolean($value);
                    }
                    when(/\bpull_aliases_from_comments\b/i) {
                        $g_use_comment_aliases = checkBoolean($value);
                    }
                    when(/\bslonik_path\b/i) {
                        $g_slonikpath = $value;
                    }
                    when(/\blockset_method\b/i) {
                        $g_lockset_method = $value;
                    }
                    when(/\benable_autofailover\b/i) {
                        $g_autofailover = checkBoolean($value);
                    }
                    when(/\bautofailover_poll_interval\b/i) {
                        $g_autofailover_poll_interval = checkInteger($value);
                    }
                    when(/\bautofailover_node_retry\b/i) {
                        $g_autofailover_retry = checkInteger($value);
                    }
                    when(/\bautofailover_sleep_time\b/i) {
                        $g_autofailover_retry_sleep = checkInteger($value);
                    }
                    when(/\bautofailover_forwarding_providers\b/i) {
                        $g_autofailover_provs = checkBoolean($value);
                    }
                    when(/\bautofailover_config_any_node\b/i) {
                        $g_autofailover_config_any = checkBoolean($value);
                    }
                    when(/\bautofailover_perspective_sleep_time\b/i) {
                        $g_autofailover_perspective_sleep = checkInteger($value);
                    }
                    when(/\bautofailover_majority_only\b/i) {
                        $g_autofailover_majority_only = checkBoolean($value);
                    }
                    when(/\bautofailover_is_quorum\b/i) {
                        $g_autofailover_is_quorum  = checkBoolean($value);
                    }
                }
            }
        }
        close (CFGFILE);

        $success = true;
    }
    else {
        println(lookupMsg('err_fail_config'));
    }

    return $success;
}

sub checkBoolean {
    my $text = shift;
    my $value = undef;
    if ( grep /^$text$/i, ("y","yes","t","true","on") ) {
        $value = true;
    }
    elsif ( grep /^$text$/i, ("n","no","f","false","off") ) {
        $value = false;
    }
    return $value;
}

sub checkInteger {
    my $integer = shift;
    my $value = undef;

    if (($integer * 1) eq $integer) {
        $value = int($integer);
    }
    return $value;
}


sub runSlonik {
    my $script = shift;
    my $prefix = shift;
    my $logfile = shift;
    my $log_prefix = shift;
    my $success;

    if ($g_debug) {
         printlogln($prefix,$logfile,$log_prefix, lookupMsg('dbg_slonik_script', $script));
    }
    if (open(SLONIKSTATUS, "-|", "slonik $script 2>&1")) {
        while (<SLONIKSTATUS>) {
            printlogln($prefix,$logfile,$log_prefix,lookupMsg('slonik_output', $_));
        }
        close(SLONIKSTATUS);
        $success = true;
    }
    else {
        printlogln($prefix,$logfile,$log_prefix, lookupMsg('err_running_slonik', $!));
        $success = false;
    }
    return $success;
}

sub autoFailover {
    my $dbconninfo = shift;
    my $clname = shift;
    my $dbuser = shift;
    my $dbpass = shift;
    my $prefix = shift;
    my $logfile = shift;
    my $log_prefix = shift;

    my $cluster_time;
    my $failed;
    my $actions;
    my $current_retry;
    my $cluster_loaded;
    my @cluster;
    my $node_count;
    my $version;

    printlogln($prefix,$logfile,$log_prefix,lookupMsg('autofailover_init'));
    printlogln($prefix,$logfile,$log_prefix,lookupMsg('autofailover_init_cnf', ($g_autofailover_config_any ? 'any' : 'specified target')));
    printlogln($prefix,$logfile,$log_prefix,lookupMsg('autofailover_init_pol', $g_autofailover_poll_interval));
    printlogln($prefix,$logfile,$log_prefix,lookupMsg('autofailover_init_ret', $g_autofailover_retry, $g_autofailover_retry_sleep));
    printlogln($prefix,$logfile,$log_prefix,lookupMsg('autofailover_init_set', ($g_autofailover_provs ? 'will' : 'will not')));

    while (true) {  
        # Probe current cluster configuration every minute 
        if (!defined($cluster_time) || (time()-$cluster_time > 60)) {

            $cluster_loaded = false;
            if (!defined($cluster_time) || !$g_autofailover_config_any) {
                eval {
                    ($node_count, $version) = loadCluster($dbconninfo, $clname, $dbuser, $dbpass, $prefix, $logfile, $log_prefix);
                    die lookupMsg('err_cluster_empty') if ($node_count == 0);
                    @cluster = @g_cluster;
                    die lookupMsg('err_cluster_lone') if ($node_count == 1);
                    $cluster_loaded = true;
                };
                if ($@) {
                    printlogln($prefix,$logfile,$log_prefix, lookupMsg('load_cluster_fail', 'from supplied configuration'));
                    if ($g_debug) {
                        printlogln($prefix,$logfile,$log_prefix,lookupMsg('dbg_generic', $@));
                    }
                }
            }
            else {
                foreach (@cluster) {
                    if ($_->[6]  eq "ACTIVE") {
                        unless ($cluster_loaded) {
                            eval {
                                ($node_count, $version) = loadCluster($_->[2], $clname, $dbuser, $dbpass, $prefix, $logfile, $log_prefix);
                                die lookupMsg('err_cluster_empty') if ($node_count == 0);
                                @cluster = @g_cluster;
                                die lookupMsg('err_cluster_lone') if ($node_count == 1);
                                $cluster_loaded = true;
                            };
                            if ($@) {
                                printlogln($prefix,$logfile,$log_prefix, lookupMsg('load_cluster_fail', 'from node ' . $_->[0] . ': trying next node'));
                                if ($g_debug) {
                                    printlogln($prefix,$logfile,$log_prefix,lookupMsg('dbg_generic', $@));
                                }
                            }
                        }
                    }
                }
            }

            if ($cluster_loaded) {
                printlogln($prefix,$logfile,$log_prefix,lookupMsg('autofailover_load_cluster', (!defined($cluster_time) ? "Loaded" : "Reloaded"), $version, $clname, $node_count));
                $cluster_time = time();
            }
            else {
                printlogln($prefix,$logfile,$log_prefix, lookupMsg('load_cluster_fail', 'from any node'));
            }
        }

        if ($cluster_loaded) {
            $current_retry = 0; 
            undef $failed;
            while(($current_retry <= $g_autofailover_retry) && ((!defined($failed)) || ($failed > 0))) {
                # Check status of cluster
                $failed = checkFailed($clname, $dbuser, $dbpass, $prefix, $logfile, $log_prefix);
                if ($failed == 0) {
                    if ($g_debug) {
                        printlogln($prefix,$logfile,$log_prefix,lookupMsg('dbg_cluster_good'));
                    }
                    if ($current_retry > 0) {
                        printlogln($prefix,$logfile,$log_prefix,lookupMsg('cluster_fixed'));
                    }
                }
                $current_retry++;
                if (($failed > 0) && ($current_retry <= $g_autofailover_retry)) {
                    printlogln($prefix,$logfile,$log_prefix,lookupMsg('cluster_failed', $failed,$g_autofailover_retry_sleep,$current_retry,$g_autofailover_retry));
                    usleep($g_autofailover_retry_sleep * 1000);
                }
            }
            if ($failed > 0) {
                if ((!$g_autofailover_majority_only || checkSplit($prefix, $logfile, $log_prefix)) && (($g_autofailover_perspective_sleep <= 0) || checkPerspective($clname, $dbuser, $dbpass, $prefix, $logfile, $log_prefix))) {
                    $actions = findBackup($clname, $dbuser, $dbpass, $prefix, $logfile, $log_prefix);
                    if ($actions > 0) {
                        printlogln($prefix,$logfile,$log_prefix,lookupMsg('autofailover_proceed'));
                        foreach my $failed ( keys %g_backups ) {
                            printlogln($prefix,$logfile,$log_prefix,lookupMsg('autofailover_detail', $failed, $g_backups{$failed}));
                        }
                        $g_script = writeFailover($prefix, $dbconninfo, $clname, $dbuser, $dbpass, undef, undef, $g_subs_follow_origin, $g_use_comment_aliases, $logfile, $log_prefix);   
                        unless (runSlonik($g_script, $prefix, $logfile, $log_prefix)) {
                            printlogln($prefix,$logfile,$log_prefix,lookupMsg('err_execute_fail', 'slonik script', $g_script));
                        }
                        $cluster_loaded = false;
                        #print "SCRIPT: $g_script\n";
                        #exit(0);
                    }
                    else {
                        printlogln($prefix,$logfile,$log_prefix,lookupMsg('autofailover_halt', $failed));
                    }
                }
            }
            usleep($g_autofailover_poll_interval * 1000);
        }
        else {
            sleep(10);
        }

    }
}

sub checkSplit {
    my $prefix = shift;
    my $logfile = shift;
    my $log_prefix = shift;

    my $majority = false; 
    my $failed = scalar(@g_unresponsive);
    my $survivers = (scalar(@g_cluster) - scalar(@g_unresponsive));

    if ($survivers > $failed) {
        $majority = true; 
        printlogln($prefix,$logfile,$log_prefix,lookupMsg('autofailover_split_check', $survivers, ($survivers+$failed)));
    }
    elsif (($survivers == $failed) && $g_autofailover_is_quorum) {
        $majority = true; 
        printlogln($prefix,$logfile,$log_prefix,lookupMsg('autofailover_split_check', ($survivers . '+quorum'), ($survivers+$failed)));
    }
    else {
        printlogln($prefix,$logfile,$log_prefix,lookupMsg('autofailover_split_check_fail', $survivers));
    }

    return $majority;
}

# Check each nodes perspective of the failure to try to ensure the issue isn't that this script just can't connect to the origin/provider
# The idea here is just to wait for a short period of time and see if the lag time for the nodes has increased by the same amount.
sub checkPerspective {
    my $clname = shift;
    my $dbuser = shift;
    my $dbpass = shift;
    my $prefix = shift;
    my $logfile = shift;
    my $log_prefix = shift;

    my $dsn;
    my $dbh;
    my $sth;
    my $query;
    my $qw_clname;
    my $param_on;
    my $agreed = false;
    my @unresponsive_ids;
    my $lag_idx;
    my $lag_confirmed;
    my @lag_info1;
    my @lag_info2;
    my $bad = 0;

    foreach (@g_unresponsive) {
        push(@unresponsive_ids, $_->[0]);
    }
    printlogln($prefix,$logfile,$log_prefix,lookupMsg('autofailover_pspec_check', join(", ", @unresponsive_ids), scalar(@g_unresponsive), scalar(@g_cluster)));

    foreach (@g_cluster) {
        unless ($_->[0] ~~ @unresponsive_ids)  {
            $dsn = "DBI:Pg:$_->[2];";
            eval {
                $dbh = DBI->connect($dsn, $dbuser, $dbpass, {RaiseError => 1});
                $qw_clname = $dbh->quote_identifier("_" . $clname);

                $query = "SELECT a.st_origin, a.st_received, extract(epoch from a.st_lag_time)::integer
                        FROM _test_replication.sl_status a
                        INNER JOIN _test_replication.sl_node b on a.st_origin = b.no_id
                        INNER JOIN _test_replication.sl_node c on a.st_received = c.no_id
                        WHERE a.st_received IN (" . substr('?, ' x scalar(@unresponsive_ids), 0, -2) . ") ORDER BY a.st_origin, a.st_received;";

                $sth = $dbh->prepare($query);

                $param_on = 1; 
                foreach (@unresponsive_ids) {
                    $sth->bind_param($param_on, $_);
                    $param_on++;
                }
                $sth->execute();

                while (my @node_lag = $sth->fetchrow) { 
                    printlogln($prefix,$logfile,$log_prefix,lookupMsg('autofailover_pspec_check_data', 'Check1', $_->[0], $node_lag[0], $node_lag[1], $node_lag[2]));
                    push(@lag_info1, \@node_lag);
                }

                $sth->finish;
                $dbh->disconnect();
            };
            if ($@) {
                printlogln($prefix,$logfile,$log_prefix,lookupMsg('autofailover_pspec_check_fail', $_->[0], $@));
                $bad++;
            } 
        }
    }

    if ($bad == 0) {
        printlogln($prefix,$logfile,$log_prefix,lookupMsg('autofailover_pspec_check_sleep', $g_autofailover_perspective_sleep));
        usleep($g_autofailover_perspective_sleep * 1000);

        foreach (@g_cluster) {
            unless ($_->[0] ~~ @unresponsive_ids)  {
                $dsn = "DBI:Pg:$_->[2];";
                eval {
                    $dbh = DBI->connect($dsn, $dbuser, $dbpass, {RaiseError => 1});
                    $qw_clname = $dbh->quote_identifier("_" . $clname);

                    $query = "SELECT a.st_origin, a.st_received, extract(epoch from a.st_lag_time)::integer
                            FROM _test_replication.sl_status a
                            INNER JOIN _test_replication.sl_node b on a.st_origin = b.no_id
                            INNER JOIN _test_replication.sl_node c on a.st_received = c.no_id
                            WHERE a.st_received IN (" . substr('?, ' x scalar(@unresponsive_ids), 0, -2) . ") ORDER BY a.st_origin, a.st_received;";

                    $sth = $dbh->prepare($query);

                    $param_on = 1;
                    foreach (@unresponsive_ids) {
                        $sth->bind_param($param_on, $_);
                        $param_on++;
                    }
                    $sth->execute();

                    while (my @node_lag = $sth->fetchrow) {
                        printlogln($prefix,$logfile,$log_prefix,lookupMsg('autofailover_pspec_check_data', 'Check2', $_->[0], $node_lag[0], $node_lag[1], $node_lag[2]));
                        push(@lag_info2, \@node_lag);
                    }

                    $sth->finish;
                    $dbh->disconnect();
                };
                if ($@) {
                    printlogln($prefix,$logfile,$log_prefix,lookupMsg('autofailover_pspec_check_fail', $_->[0], $@));
                    $bad++;
                }
            }
        }

        $lag_idx = 0;
        $lag_confirmed = 0;
        foreach (@lag_info1) {
            if ($g_debug) {
                printlogln($prefix,$logfile,$log_prefix,lookupMsg('dbg_generic', ("Node $_->[0] lag between checks on node $_->[1] is " . ($lag_info2[$lag_idx]->[2]-$_->[2]) . " seconds")));
            }

            if ((($lag_info2[$lag_idx]->[2]-$_->[2])*1000) >= $g_autofailover_perspective_sleep) {
                $lag_confirmed++;
            }
            $lag_idx++;
        }  
    }

    if ($bad > 0) {
        printlogln($prefix,$logfile,$log_prefix,lookupMsg('autofailover_pspec_check_unknown'));
    }   
    elsif ($lag_idx == $lag_confirmed) {
        printlogln($prefix,$logfile,$log_prefix,lookupMsg('autofailover_pspec_check_true'));
        $agreed = true;
    } 
    else {
        printlogln($prefix,$logfile,$log_prefix,lookupMsg('autofailover_pspec_check_false'));
    }

    return $agreed;
}

sub checkFailed {
    my $clname = shift;
    my $dbuser = shift;
    my $dbpass = shift;
    my $prefix = shift;
    my $logfile = shift;
    my $log_prefix = shift;

    my $dsn;
    my $dbh;
    my $sth;
    my $query;
    my $result_count = 0;
    my $prov_failed = 0;
    my $subonly_failed = 0;

    undef @g_unresponsive;

    foreach (@g_cluster) {
        if ($_->[6] eq "ACTIVE") {
            if ($g_debug) {
                printlogln($prefix,$logfile,$log_prefix,lookupMsg('dbg_autofailover_check',$_->[0], ($_->[4] // "unnamed"),(defined($_->[9]) ? "provider of sets $_->[9]" : "sole subscriber"),$_->[2]));
            }

            if ($g_debug) {
                if ((defined($_->[3])) || ($g_autofailover_provs && defined($_->[9]))) {
                    printlogln($prefix,$logfile,$log_prefix,lookupMsg('dbg_autofailover_active_check', 'provider', $_->[0]));
                }
                else {
                    printlogln($prefix,$logfile,$log_prefix,lookupMsg('dbg_autofailover_active_check', 'subscriber only', $_->[0]));
                }
            }

            $dsn = "DBI:Pg:$_->[2];";
            eval {
                $dbh = DBI->connect($dsn, $dbuser, $dbpass, {RaiseError => 1});
                $query = "SELECT count(*) FROM pg_namespace WHERE nspname = ?";
                $sth = $dbh->prepare($query);
                $sth->bind_param(1, "_" . $clname);
                $sth->execute();

                $result_count = $result_count+$sth->rows;
    
                $sth->finish;
                $dbh->disconnect();
            };
            if ($@) {
                if ($g_debug) {
                    printlogln($prefix,$logfile,$log_prefix,lookupMsg('dbg_generic', $@));
                }
                push(@g_unresponsive, \@$_); 
                if ((defined($_->[3])) || ($g_autofailover_provs && defined($_->[9]))) {
                    printlogln($prefix,$logfile,$log_prefix,lookupMsg('autofailover_unresponsive', $_->[0]));
                    unless ($g_failover_method ne 'new' && !defined($_->[3])) {
                        $prov_failed++;
                    }
                }
                else {
                    printlogln($prefix,$logfile,$log_prefix,lookupMsg('autofailover_unresponsive_subonly', $_->[0]));
                    if ($g_fail_subonly) {
                        $subonly_failed++;
                    }
                }
            }
        }
        else {
            if ($g_debug) {
                printlogln($prefix,$logfile,$log_prefix,lookupMsg('dbg_autofailover_check',$_->[0], ($_->[4] // "unnamed"), lc($_->[6] // "unknown") .  ' node', $_->[2]));
            }
        }
    }
    if ($prov_failed > 0) {
        return ($prov_failed+$subonly_failed);
    }
    else {
        return $prov_failed;
    }
}

sub findBackup {
    my $clname = shift;
    my $dbuser = shift;
    my $dbpass = shift;
    my $prefix = shift;
    my $logfile = shift;
    my $log_prefix = shift;

    my $dsn;
    my $dbh;
    my $sth;
    my $query;
    my $qw_clname;
    my $result_count = 0;
    my $lowest_lag_time;
    my $lowest_lag_events;
    my $best_node_id;    
    my $best_node_is_direct;    
    my @sets_from;
    my @sets_to;
    my %backup_for_set_chosen;

    undef %g_backups;
    undef @g_failed;

    foreach (@g_unresponsive) {
        if ($g_fail_subonly || (defined($_->[3])) || ($g_autofailover_provs && defined($_->[9]))) {
            printlogln($prefix,$logfile,$log_prefix,lookupMsg('autofailover_promote_find', ($_->[9] // "none"), $_->[0]));

            undef $best_node_id;
            $lowest_lag_time = (1<<$Config{ivsize}*8-1)-1;
            $lowest_lag_events = $lowest_lag_time;

            if (defined($_->[9]) && (exists $backup_for_set_chosen{$_->[9]})) {
                $best_node_id = $backup_for_set_chosen{$_->[9]};
                printlogln($prefix,$logfile,$log_prefix,lookupMsg('autofailover_promote_found', $_->[9], $_->[0]));
            }
            else {
                foreach my $subscriber (@g_cluster) {
                    if ($subscriber->[0] != $_->[0]) {
                        if ($g_debug) {
                            printlogln($prefix,$logfile,$log_prefix,lookupMsg('autofailover_check_sub',$subscriber->[0]));
                        }
    
                        $dsn = "DBI:Pg:$subscriber->[2]";
        
                        eval {
                            $dbh = DBI->connect($dsn, $dbuser, $dbpass, {RaiseError => 1});
                            $qw_clname = $dbh->quote_identifier("_" . $clname);

                            $query = "SELECT extract(epoch from a.st_lag_time), a.st_lag_num_events, (a.st_received = ?) AS direct
                                FROM $qw_clname.sl_status a
                                INNER JOIN $qw_clname.sl_subscribe b ON b.sub_provider = a.st_received AND b.sub_receiver = a.st_origin
                                WHERE b.sub_active 
                                GROUP BY a.st_lag_time, a.st_lag_num_events, a.st_received;";

                            $sth = $dbh->prepare($query);
                            $sth->bind_param(1, $_->[0]);
                            $sth->execute();

                            while (my @subinfo = $sth->fetchrow) {

                                undef @sets_from;
                                if (defined($_->[9])) {
                                    printlogln($prefix,$logfile,$log_prefix,lookupMsg('autofailover_node_detail', $subscriber->[0], ($subinfo[2]?"directly":"indirectly"), (defined($_->[3])?"origin":"provider"), $_->[0], $subscriber->[7], $subinfo[0], $subinfo[1]));
                                    @sets_from = split(',',$_->[9]);
                                    @sets_to = split(',',$subscriber->[7]);
                                }
                                elsif ($g_fail_subonly) {
                                    # Subscriber only node will have no active sets forwarding sets to check
                                    printlogln($prefix,$logfile,$log_prefix,lookupMsg('autofailover_node_detail_subonly', $subscriber->[0], ($subinfo[2]?"directly":"indirectly"), (defined($_->[3])?"origin":"provider"), $_->[0], $subinfo[0], $subinfo[1]));
                                    @sets_from = (0);
                                    @sets_to = (0);
                                }

                                if ((checkProvidesAllSets(\@sets_from, \@sets_to)) && (($subinfo[0] < $lowest_lag_time && ($subinfo[2] || !defined($best_node_id))) || (!$best_node_is_direct && $subinfo[2]))) {
                                    printlogln($prefix,$logfile,$log_prefix,lookupMsg('autofailover_promote_best', $subscriber->[0], $subinfo[0], $subinfo[1]));
                                    $best_node_id = $subscriber->[0];
                                    $lowest_lag_time = $subinfo[0];
                                    $lowest_lag_events = $subinfo[1];
	                            $best_node_is_direct = $subinfo[2];
                                }
                            }
                        };
                        if ($@) {
                            printlogln($prefix,$logfile,$log_prefix,lookupMsg('autofailover_check_sub_fail', $subscriber->[0]));
                            if ($g_debug) {
                                printlogln($prefix,$logfile,$log_prefix,lookupMsg('dbg_generic', $@));
                            }
                        }
                    }
                }
            }
            if (defined($best_node_id)) { 
                push(@g_failed, \@$_);
                $g_backups{$_->[0]} = $best_node_id;
                if (defined($_->[9]) && !(exists $g_backups{$_->[9]})) {
                    $backup_for_set_chosen{$_->[9]} = $best_node_id;
                }
            }
            else {
                printlog($prefix,$logfile,$log_prefix,lookupMsg('autofailover_promote_fail')); 
            }
        }
        else {
            printlogln($prefix,$logfile,$log_prefix,lookupMsg('autofailover_promote_skip', $_->[0]));
        }
    }
    return keys(%g_backups);
}
