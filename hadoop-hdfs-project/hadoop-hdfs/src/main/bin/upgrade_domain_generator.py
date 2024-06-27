import csv
import getopt
import json
import sys
import time

HOST_KEY = "hostName"
UPGRADE_DOMAIN_KEY = "upgradeDomain"
UPGRADE_DOMAIN_PREFIX = "ud"

hosts_list = []
dfs_hosts_list = []
topology_mapping = {}
dc_ud_host_map = {}
upgrade_domain_stat = {}


# load host list file
def load_host_list( host_file ):
    with open(host_file, 'r') as f_hosts:
        for line in f_hosts:
            hosts_list.append(list(line.strip('\n').split(','))[0])


# load topology info
def load_topology_info( topo_file ):
    with open(topo_file) as f:
        r = csv.reader(f)
        for row in r:
            topology_mapping.update({row[0]: '/' + '/'.join(row[3:5]) + '-' + row[5]})
            topology_mapping.update({row[1]: '/' + '/'.join(row[3:5]) + '-' + row[5]})


# load original dfs hosts file
def load_dfs_hosts( dfs_hosts ):
    with open(dfs_hosts, 'r') as f_hosts:
        records = json.load(f_hosts)

    for record in records:
        try:
            current_dc = get_host_dc(record[HOST_KEY])
            # Check nodes put in the wrong upgrade domain
            if record[UPGRADE_DOMAIN_KEY].split('/')[1] != current_dc:
                print("Warning: " + record[HOST_KEY] + " upgrade domain doesn't match topo!")
                current_dc = record[UPGRADE_DOMAIN_KEY].split('/')[1]
            if current_dc not in dc_ud_host_map.keys():
                dc_ud_host_map[current_dc] = {}
                upgrade_domain_stat[current_dc] = {}
            if record[UPGRADE_DOMAIN_KEY] not in dc_ud_host_map[current_dc].keys():
                dc_ud_host_map[current_dc][record[UPGRADE_DOMAIN_KEY]] = []
                upgrade_domain_stat[current_dc][record[UPGRADE_DOMAIN_KEY]] = 0
            dc_ud_host_map[current_dc][record[UPGRADE_DOMAIN_KEY]].append(record[HOST_KEY])
            upgrade_domain_stat[current_dc][record[UPGRADE_DOMAIN_KEY]] += 1
            dfs_hosts_list.append(record[HOST_KEY])
        except:
            print("Format error for " + record[HOST_KEY])


def init( domain_count ):
    for dc in domain_count.keys():
        # tool doesn't allow to add new upgrade domain on existed dc
        if dc in dc_ud_host_map.keys():
            print("Data center: \"" + dc + "\" has been created upgrade domain. Exiting...")
            sys.exit(3)
        # initialize upgrade domain for given dc
        dc_ud_host_map[dc] = {}
        upgrade_domain_stat[dc] = {}
        for i in range(domain_count[dc]):
            cur_upgrade_domain = "/" + dc + "/" + UPGRADE_DOMAIN_PREFIX + str(i)
            upgrade_domain_stat[dc][cur_upgrade_domain] = 0
            dc_ud_host_map[dc][cur_upgrade_domain] = []

    match()


def add( add_count ):
    for dc in add_count.keys():
        # find the max index of upgrade domain
        max_suffix = -1
        for upgrade_domain in upgrade_domain_stat[dc].keys():
            cur_suffix = int(upgrade_domain.lstrip("/" + dc + "/" + UPGRADE_DOMAIN_PREFIX))
            max_suffix = max(max_suffix, cur_suffix)
        for i in range(add_count[dc]):
            # initialize the new upgrade domain
            cur_upgrade_domain = "/" + dc + "/" + UPGRADE_DOMAIN_PREFIX + (i + max_suffix + 1)
            print("Process upgrade domain: " + cur_upgrade_domain)
            upgrade_domain_stat[dc][cur_upgrade_domain] = 0
            dc_ud_host_map[dc][cur_upgrade_domain] = []

            # sort for the original upgrade domains
            upgrade_domain_stat_internal = sorted(upgrade_domain_stat[dc].items(), key=lambda x: x[1], reverse=True)
            min_upgrade_domain_count = upgrade_domain_stat_internal[-1][1]
            while upgrade_domain_stat[dc][cur_upgrade_domain] < min_upgrade_domain_count:
                # get a host from the biggest upgrade domain and delete from the original domain
                max_upgrade_domain = upgrade_domain_stat_internal[0][0]
                dc_ud_host_map[dc][cur_upgrade_domain].append(
                    dc_ud_host_map[dc][max_upgrade_domain][0])
                dc_ud_host_map[dc][max_upgrade_domain].delete(0)
                upgrade_domain_stat[dc][max_upgrade_domain] -= 1
                upgrade_domain_stat[dc][cur_upgrade_domain] += 1
                upgrade_domain_stat_internal = sorted(upgrade_domain_stat[dc].items(), key=lambda x: x[1], reverse=True)
                min_upgrade_domain_count = upgrade_domain_stat_internal[-1][1]


def match():
    global hosts_list, upgrade_domain_stat

    for host in hosts_list:
        # host already in dfs hosts map
        if host in dfs_hosts_list:
            print(host + " already in dfs.hosts file")
            continue
        # check if node's dc has upgrade domain or not
        current_dc = get_host_dc(host)
        if current_dc not in dc_ud_host_map.keys():
            print("host: \"" + host + "\"" + " dc: \"" + current_dc + "\" doesn't have upgrade domain!")
            continue

        upgrade_domain_stat_internal = sorted(upgrade_domain_stat[current_dc].items(), key=lambda x: x[1],
                                              reverse=False)
        good_domain = False
        for tmp in upgrade_domain_stat_internal:
            current_domain = tmp[0]
            # empty upgrade domain just add host in
            if not dc_ud_host_map[current_dc][current_domain]:
                good_domain = True
                break
            else:
                for domain_host in dc_ud_host_map[current_dc][current_domain]:
                    good_domain = True
                    if topology_mapping[domain_host] == topology_mapping[host]:
                        good_domain = False
                        break
                if good_domain:
                    break
        # domain number is too little, host cannot find a suitable domain, create a new domain
        if not good_domain:
            print(current_dc + " doesn't have enough upgrade domain, will create a new upgrade domain")
            current_domain = get_next_upgrade_domain(current_dc)
            dc_ud_host_map[current_dc][current_domain] = []
            upgrade_domain_stat[current_dc][current_domain] = 0
        # update upgrade domain info
        dc_ud_host_map[current_dc][current_domain].append(host)
        upgrade_domain_stat[current_dc][current_domain] += 1
        dfs_hosts_list.append(host)


def output( hosts_file ):
    print("Summary:")
    for dc in upgrade_domain_stat.keys():
        print(dc + ':')
        print(str(upgrade_domain_stat[dc]))

    with open(hosts_file, 'w') as fout:
        fout.write("[\n")
        output_string = ""
        for dc in dc_ud_host_map.keys():
            for upgrade_domain in dc_ud_host_map[dc].keys():
                for host in dc_ud_host_map[dc][upgrade_domain]:
                    output_string += ("{\"" + HOST_KEY + "\": \"" + host + "\", \"" + UPGRADE_DOMAIN_KEY + "\": \""
                                      + upgrade_domain + "\"},\n")
        fout.write(output_string[:-2] + "\n")
        fout.write("]")


def get_next_upgrade_domain( dc ):
    max_suffix = -1
    try:
        for upgrade_domain in upgrade_domain_stat[dc].keys():
            cur_suffix = int(upgrade_domain.split('/')[2].lstrip(UPGRADE_DOMAIN_PREFIX))
            max_suffix = max(max_suffix, cur_suffix)
    except:
        print(upgrade_domain)
        sys.exit()
    return "/" + dc + "/" + UPGRADE_DOMAIN_PREFIX + str(max_suffix + 1)


def parse_domain_number( arg, domain_number ):
    for item in arg.strip().split(','):
        item_list = item.strip().split(':')
        if item_list[0].startswith('/'):
            item_list[0] = item_list[0][1:]
        domain_number.update({item_list[0]: int(item_list[1])})


def get_host_dc( host ):
    return topology_mapping[host].split('/')[1]


def main() -> object:
    start = time.time()
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h:m:i:t:d:n:o",
                                   ["mode=", "inputlist=", "topology=", "dfshosts=", "number="])
    except getopt.GetoptError:
        print('upgrade_domain_generator.py -m <init/add/match> '
              '-i <inputlist> -t <topology> -d <dfshosts>')
        sys.exit(2)

    mode = ""
    input_file = "hosts.list"
    topology_file = "topology-mappings.csv"
    dfs_hosts_file = ""
    output_file = "dfs.hosts"
    domain_number = {}

    for opt, arg in opts:
        if opt == '-h':
            print('upgrade_domain_generator.py -m <init/add/match> '
                  '-i <inputlist> -t <topology> -d <dfshosts> -o <outputfile> -n <number>')
            sys.exit()
        elif opt in ("-m", "--mode"):
            mode = arg
        elif opt in ("-i", "--inputlist"):
            input_file = arg
        elif opt in ("-t", "--topology"):
            topology_file = arg
        elif opt in ("-d", "--dfshosts"):
            dfs_hosts_file = arg
            output_file = dfs_hosts_file
        elif opt in ("-n", "--number"):
            parse_domain_number(arg, domain_number)
        elif opt in ("-o", "--outputfile"):
            output_file = arg

    if mode == 'init':
        print("Running in init mode!")
        load_host_list(input_file)
        load_topology_info(topology_file)
        if dfs_hosts_file != "":
            load_dfs_hosts(dfs_hosts_file)
        init(domain_number)
    else:
        print("Running in match mode!")
        load_host_list(input_file)
        load_topology_info(topology_file)
        load_dfs_hosts(dfs_hosts_file)
        match()
    output(output_file)

    print("Runtime is " + str(time.time() - start) + 's')


if __name__ == "__main__":
    main()