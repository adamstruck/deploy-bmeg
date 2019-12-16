import argparse
import csv
import gzip
import logging
import multiprocessing
import os
import re
import types
import ujson

from flatten_json import flatten


def reader(path):
    if path.endswith('.gz'):
        return gzip.open(path, 'r')
    else:
        return open(path, 'r')


def to_vertex(path):
    """ vertex with only scalar data """
    with reader(path) as ins:
        for line in ins:
            line = flatten(ujson.loads(line), '.')
            del line['_id']
            del line['label']
            yield line


def to_edge(path):
    """ edge with only scalar data """
    with reader(path) as ins:
        for line in ins:
            line = flatten(ujson.loads(line), '.')
            # not used
            del line['_id']
            del line['gid']
            # rename from, to, label
            # line[':START_ID'] = line['from']
            # line[':END_ID'] = line['to']
            # line[':TYPE'] = line['label']
            # del line['label']
            # del line['from']
            # del line['to']
            yield line


def keys(path, sample_size=1000):
    """ return [names of keys] and [neo types of keys]"""
    if 'Expression' in path or 'CopyNumber' in path:
        sample_size = 1  # no need to read huge, uniform records
    kv_scheme = {}
    py_2_neo = {
        'str': 'string',
        'bool': 'boolean',
        'int': 'long',
        'float': 'float'
    }  # xlate py types to neo4j
    c = 0
    xformer = to_vertex
    if 'Edge' in path:
        xformer = to_edge
    for line in xformer(path):
        kv_scheme.update({k: line[k].__class__.__name__ for k in line.keys() if line[k] is not None})
        if c == sample_size:
            break
        c += 1

    decorated_keys = []
    keys = list(kv_scheme.keys())
    for key in keys:
        py_type = kv_scheme[key]
        # ignore lists, dicts, etc... flatten_json should have taken care of most of these
        if py_type not in py_2_neo:
            continue
        value_type = py_2_neo[py_type]
        if key == 'gid':
            value_type = 'ID'
        elif key == "from":
            key = ''
            value_type = 'START_ID'
        elif key == "to":
            key = ''
            value_type = 'END_ID'
        elif key == "label":
            key = ''
            value_type = 'TYPE'
        decorated_keys.append('{}:{}'.format(key, value_type))
    return keys, decorated_keys


def values(path):
    """ return a dict for each line """
    xformer = to_vertex
    if 'Edge' in path:
        xformer = to_edge
    for line in xformer(path):
        yield line


first_cap_re = re.compile('(.)([A-Z][a-z]+)')
all_cap_re = re.compile('([a-z0-9])([A-Z])')
def to_snakecase(label):
    # handle G2P exception
    label = label.replace("G2P", "g2p")
    s1 = first_cap_re.sub(r'\1_\2', label)
    return all_cap_re.sub(r'\1_\2', s1).lower()


def get_label(path):
    """ given path, return label """
    path = os.path.basename(path)
    if 'Vertex' in path:
        file_parts = path.split('.')
        return file_parts[file_parts.index('Vertex') - 1]
    elif 'Edge' in path:
        edge_composite = re.search('(^.*\.)?(.*_.*_.*)\.Edge.json*', path).group(2)
        return to_snakecase(edge_composite.split('_')[1])
    else:
        raise TypeError("not a vertex or edge file: {}".format(path))


def to_csv_header(path, output=None):
    """ file to csv '{path}.header.csv' """
    fieldnames, decorated_fieldnames = keys(path)
    return dict(zip(fieldnames, decorated_fieldnames))


def get_output_path(outdir, path):
    return os.path.join(outdir, '{}.csv'.format(path.replace('/', '.')))


def to_csv(input, output, header, limit=None, write_header=False):
    """ file to csv '{path}.csv' """
    replace_str = lambda x: str(x).strip().replace('\n', '').replace('\r', '')  if x is not None else None
    neo_2_py = {
        'string': replace_str,
        'boolean': lambda x: bool(x) if x is not None else None,
        'long': lambda x: int(x) if x is not None else None,
        'float': lambda x: float(x) if x is not None else None,
        'ID': replace_str,
        'START_ID': replace_str,
        'END_ID': replace_str,
        'TYPE': replace_str,
    }
    with open(header, 'r') as fh:
        reader = csv.DictReader(fh)
        fnames = reader.fieldnames
        fieldnames = []
        types = {}
        for x in fnames:
            f, t = x.split(":")
            if t == "ID":
                f = "gid"
            if t == "TYPE":
                f = "label"
            elif t == "START_ID":
                f = "from"
            elif t == "END_ID":
                f = "to"
            fieldnames.append(f)
            types[f] = t
    with open(output, 'w', newline='') as myfile:
        writer = csv.DictWriter(myfile, fieldnames=fieldnames, extrasaction='raise')
        if write_header:
            writer.writeheader()
        c = 0
        for line in values(input):
            row = {k: neo_2_py[types[k]](line[k]) for k in fieldnames if k in line}
            writer.writerow(row)
            c += 1
            if limit and c == limit:
                break
        logging.info('wrote {} records to {}'.format(c, output))
    return output


def to_csv_job(path, outdir, limit=None):
    """ cmd line to transform json to csv """
    output_path = get_output_path(outdir, path)
    label = get_label(path)
    typ = 'Vertex' if 'Vertex' in path else 'Edge'
    label = '{}.{}'.format(label, typ)
    header_path = os.path.join(outdir, '{}.header.csv'.format(label))
    comment = ''
    if os.path.isfile(output_path):
        comment = '# '
    if limit:
        limit = '--limit {}'.format(limit)
    else:
        limit = ''
    script_dir = os.path.dirname(os.path.realpath(__file__))
    return '{}python3.7 {}/to_csv.py convert --input {} --output {} --header {} {}'.format(comment, script_dir, path, output_path, header_path, limit)


def cmd_gen(manifest, db_name, cmd_outdir, csv_outdir, limit):
    """render csv file(s) and neo4j-import clause"""

    config = {
        'edge_files': [],
        'vertex_files': [],
    }
    config = types.SimpleNamespace(**config)

    os.makedirs(cmd_outdir, exist_ok=True)
    os.makedirs(csv_outdir, exist_ok=True)

    with open(manifest, 'r') as stream:
        for line in stream:
            line = line.strip()
            if 'Edge' in line:
                config.edge_files.append(line)
            elif 'Vertex' in line:
                config.vertex_files.append(line)

    vertex_csvs = {}
    edge_csvs = {}
    to_csv_commands = []
    headers = {}
    # read all files to determine header by label
    for path in config.vertex_files + config.edge_files:
        if not os.path.isfile(path):
            logging.warning('{} does not exist'.format(path))
            continue
        label = get_label(path)
        typ = 'Vertex' if 'Vertex' in path else 'Edge'
        label = '{}.{}'.format(label, typ)
        if label not in headers:
            headers[label] = {}
        headers[label] = {**headers[label], **to_csv_header(path)}
    # write csv header files
    for label in headers.keys():
        output_path = os.path.join(csv_outdir, '{}.header.csv'.format(label))
        with open(output_path, "w", newline='') as myfile:
            writer = csv.DictWriter(myfile, fieldnames=headers[label].keys())
            writer.writerow(headers[label])

    for path in config.vertex_files:
        if not os.path.isfile(path):
            logging.warning('{} does not exist'.format(path))
            continue
        label = get_label(path)
        if label not in vertex_csvs:
            vertex_csvs[label] = []
            vertex_csvs[label].append(os.path.join(csv_outdir, '{}.Vertex.header.csv'.format(label)))
        to_csv_commands.append(to_csv_job(path, csv_outdir, limit=limit))
        vertex_csvs[label].append(get_output_path(csv_outdir, path))

    for path in config.edge_files:
        if not os.path.isfile(path):
            logging.warning('{} does not exist'.format(path))
            continue
        label = get_label(path)
        if label not in edge_csvs:
            edge_csvs[label] = []
            edge_csvs[label].append(os.path.join(csv_outdir, '{}.Edge.header.csv'.format(label)))
        to_csv_commands.append(to_csv_job(path, csv_outdir, limit=limit))
        edge_csvs[label].append(get_output_path(csv_outdir, path))

    path = os.path.join(cmd_outdir, 'to_csv_commands.txt')
    with open(path, 'w') as outfile:
        for command in to_csv_commands:
            outfile.write('{}\n'.format(command))
        logging.info('wrote {}'.format(path))

    nodes = []
    for key in vertex_csvs.keys():
        nodes.append('--nodes:{} {}'.format(key, ','.join(vertex_csvs[key])))

    edges = []
    for key in edge_csvs.keys():
        edges.append('--relationships:{} {}'.format(key, ','.join(edge_csvs[key])))

    cmds = '\n'.join([
        'parallel --jobs {} < {}'.format(multiprocessing.cpu_count(), os.path.join(cmd_outdir, "to_csv_commands.txt")),
        'neo4j-admin import --database {} --ignore-missing-nodes=true --ignore-duplicate-nodes=true --ignore-extra-columns=true --high-io=true \\'.format(db_name)
    ])
    cmds = '{}\n  {}\n'.format(cmds, ' \\\n  '.join(nodes + edges))
    path = os.path.join(cmd_outdir, 'load_db.txt')
    with open(path, 'w') as outfile:
        outfile.write(cmds)
        logging.info('wrote {}'.format(path))


if __name__ == '__main__':  # pragma: no cover
    logging.getLogger().setLevel(logging.DEBUG)
    parser = argparse.ArgumentParser(description='Loads vertexes and edges into neo4j')
    parser.add_argument('--limit', dest='limit', type=int, default=None, help='limit the number of rows in each vertex/edge')
    subparsers = parser.add_subparsers(help='sub-command help')
    cmdgen_parser = subparsers.add_parser('cmd-gen', help='generate to_csv commands')
    cmdgen_parser.add_argument('--manifest', dest='manifest', required=True, help='manifest file path')
    cmdgen_parser.add_argument('--db-name', dest='db_name', default='bmeg.db', help='directory name')
    cmdgen_parser.add_argument('--cmd-outdir', dest='cmd_outdir', default='.', help='directory in which to write command files (to_csv_commands.txt, load_db.txt)')
    cmdgen_parser.add_argument('--csv-outdir', dest='csv_outdir', default='.', help='directory in which commands should specify to write csv files')
    cmdgen_parser.set_defaults(func=cmd_gen)
    # config_path = '{}/config.yml'.format(os.path.dirname(os.path.realpath(__file__)))
    # parser.add_argument('--config', dest='config', default=config_path, help='config path {}'.format(config_path))
    tocsv_parser = subparsers.add_parser('convert', help='convert input json to csv')
    tocsv_parser.add_argument('--limit', dest='limit', type=int, default=None, help='limit the number of rows in each vertex/edge [default: None]')
    tocsv_parser.add_argument('--input', dest='input', required=True, help='path for single input file')
    tocsv_parser.add_argument('--output', dest='output', required=True, help='path for single output file')
    tocsv_parser.add_argument('--header', dest='header', required=True, help='path to corresponding header file')
    tocsv_parser.set_defaults(func=to_csv)
    args = parser.parse_args()
    logging.debug(vars(args))
    cmd_args = vars(args).copy()
    del cmd_args['func']
    args.func(**cmd_args)
