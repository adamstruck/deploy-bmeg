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
            yield line


def to_edge(path):
    """ edge with only scalar data """
    with reader(path) as ins:
        for line in ins:
            line = flatten(ujson.loads(line), '.')
            del line['_id']
            yield line


def keys(path, sample_size=1000):
    """ return [names of keys] and [types of keys]"""
    if 'Expression' in path or 'CopyNumber' in path:
        sample_size = 1  # no need to read huge, uniform records
    kv_scheme = {}
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
        if py_type not in ['str', 'int', 'float', 'bool']:
            continue
        decorated_keys.append('{}:{}'.format(key, py_type))
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


def to_header_dict(path, output=None):
    """ generate header dictionary """
    fieldnames, decorated_fieldnames = keys(path)
    return dict(zip(fieldnames, decorated_fieldnames))


def get_output_path(outdir, path):
    return os.path.join(outdir, '{}.rdf'.format(path.replace('/', '.').strip('.')))


def to_rdf(input, output, header, limit=None):
    """ file to rdf '{path}.rdf' """
    convert = {
        'str': lambda x: '"{}"'.format(str(x).strip().replace('\n', '').replace('\r', ''))  if x is not None else None,
        'bool': lambda x: bool(x) if x is not None else None,
        'int': lambda x: int(x) if x is not None else None,
        'float': lambda x: float(x) if x is not None else None,
    }
    with open(header, 'r') as fh:
        reader = csv.DictReader(fh)
        fnames = reader.fieldnames
        fieldnames = []
        types = {}
        for x in fnames:
            f, t = x.split(":")
            fieldnames.append(f)
            types[f] = t
    with open(output, 'w') as writer:
        c = 0
        for line in values(input):
            gid = line['gid'].replace(':', '-')
            del line['gid']
            row = {k: convert[types[k]](line[k]) for k in fieldnames if k in line}
            if 'Edge' in input:
                writer.write('_:{} <{}> {}'.format(row['from'].replace(':', '-'), row['label'], row['to'].replace(':', '-')))
                del row['from']
                del row['to']
            del row['label']
            for k, v in row.items():
                if v is None:
                    continue
                rdf = '_:{} <{}> {}\n'.format(gid, k, v)
                writer.write(rdf)
            c += 1
            if limit and c == limit:
                break
        logging.info('wrote {} records to {}'.format(c, output))
    return output


def to_rdf_job(path, outdir, limit=None):
    """ cmd line to transform json to rdf """
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
    return '{}python3.7 {}/to_rdf.py convert --input {} --output {} --header {} {}'.format(comment, script_dir, path, output_path, header_path, limit)

def cmd_gen(manifest, cmd_outdir, data_outdir, limit):
    """ render commands to generate rdf file(s) and for for loading them into dgraph """

    config = {
        'edge_files': [],
        'vertex_files': [],
    }
    config = types.SimpleNamespace(**config)

    with open(manifest, 'r') as stream:
        for line in stream:
            line = line.strip()
            if 'Edge' in line:
                config.edge_files.append(line)
            else:
                config.vertex_files.append(line)

    vertex_rdfs = {}
    edge_rdfs = {}
    to_rdf_commands = []
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
        headers[label] = {**headers[label], **to_header_dict(path)}
    # write csv header files
    for label in headers.keys():
        output_path = os.path.join(data_outdir, '{}.header.csv'.format(label))
        with open(output_path, "w", newline='') as myfile:
            writer = csv.DictWriter(myfile, fieldnames=headers[label].keys())
            writer.writerow(headers[label])

    for path in config.vertex_files:
        if not os.path.isfile(path):
            logging.warning('{} does not exist'.format(path))
            continue
        label = get_label(path)
        if label not in vertex_rdfs:
            vertex_rdfs[label] = []
            vertex_rdfs[label].append(os.path.join(data_outdir, '{}.Vertex.header.csv'.format(label)))
        to_rdf_commands.append(to_rdf_job(path, data_outdir, limit=limit))
        vertex_rdfs[label].append(get_output_path(data_outdir, path))

    for path in config.edge_files:
        if not os.path.isfile(path):
            logging.warning('{} does not exist'.format(path))
            continue
        label = get_label(path)
        if label not in edge_rdfs:
            edge_rdfs[label] = []
            edge_rdfs[label].append(os.path.join(data_outdir, '{}.Edge.header.csv'.format(label)))
        to_rdf_commands.append(to_rdf_job(path, data_outdir, limit=limit))
        edge_rdfs[label].append(get_output_path(data_outdir, path))

    path = os.path.join(cmd_outdir, 'to_rdf_commands.txt')
    with open(path, 'w') as outfile:
        for command in to_rdf_commands:
            outfile.write('{}\n'.format(command))
        logging.info('wrote {}'.format(path))
    # TODO: commands to load into db


if __name__ == '__main__':  # pragma: no cover
    logging.getLogger().setLevel(logging.DEBUG)
    parser = argparse.ArgumentParser(description='Loads vertexes and edges into dgraph')
    parser.add_argument('--limit', dest='limit', type=int, default=None, help='limit the number of rows in each vertex/edge')
    subparsers = parser.add_subparsers(help='sub-command help')
    cmdgen_parser = subparsers.add_parser('cmd-gen', help='generate to_rdf commands')
    cmdgen_parser.add_argument('--manifest', dest='manifest', required=True, help='manifest file path')
    cmdgen_parser.add_argument('--cmd-outdir', dest='cmd_outdir', default='.', help='directory in which to write command files (to_rdf_commands.txt, load_db.txt)')
    cmdgen_parser.add_argument('--data-outdir', dest='data_outdir', default='.', help='directory in which commands should specify to write rdf files')
    cmdgen_parser.set_defaults(func=cmd_gen)
    tordf_parser = subparsers.add_parser('convert', help='convert input json to RDF')
    tordf_parser.add_argument('--limit', dest='limit', type=int, default=None, help='limit the number of rows in each vertex/edge [default: None]')
    tordf_parser.add_argument('--input', dest='input', required=True, help='path for single input file')
    tordf_parser.add_argument('--output', dest='output', required=True, help='path for single output file')
    tordf_parser.add_argument('--header', dest='header', required=True, help='path to corresponding header file')
    tordf_parser.set_defaults(func=to_rdf)
    args = parser.parse_args()
    logging.debug(vars(args))
    cmd_args = vars(args).copy()
    del cmd_args['func']
    args.func(**cmd_args)
