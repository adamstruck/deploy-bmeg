import argparse
import gzip
import logging
import multiprocessing
import os
import re
import sys
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


def to_rdf(input, output, schema, limit=None):
    """ file to rdf '{path}.rdf' """
    convert = {
        'string': lambda x: str(x).strip().replace('\n', '').replace('\r', '') if x is not None else None,
        'bool': lambda x: bool(x) if x is not None else None,
        'int': lambda x: int(x) if x is not None else None,
        'float': lambda x: float(x) if x is not None else None,
    }
    fieldnames = []
    types = {}
    with open(schema, 'r') as fh:
        for line in fh:
            line = line.split(' ')
            f = line[0].strip(':')
            t = line[1]
            fieldnames.append(f)
            types[f] = t
    with open(output, 'w') as writer:
        c = 0
        for line in values(input):
            row = {k: convert[types[k]](line[k]) for k in fieldnames if k in line and k not in ['gid', 'label', 'from', 'to']}
            if 'Edge' in input:
                writer.write('_:{} <{}> _:{}'.format(line['from'].replace(':', '-'), line['label'], line['to'].replace(':', '-')))
                if len(row) > 0:
                    attrs = []
                    for k, v in row.items():
                        attrs.append('{}={},'.format(k, v))
                    writer.write(' ({})'.format(', '.join(attrs)))
                writer.write(' .\n')
            else:
                gid = line['gid'].replace(':', '-')
                # https://docs.dgraph.io/howto/#giving-nodes-a-type
                writer.write('_:{} <label.{}> "" .\n'.format(gid, line['label']))
                for k, v in row.items():
                    if v is None:
                        continue
                    rdf = '_:{} <{}> "{}" .\n'.format(gid, k, v)
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
    schema_path = os.path.join(outdir, '{}.schema.rdf'.format(label))
    comment = ''
    if os.path.isfile(output_path):
        comment = '# '
    if limit:
        limit = '--limit {}'.format(limit)
    else:
        limit = ''
    script_dir = os.path.dirname(os.path.realpath(__file__))
    return '{}python3.7 {}/to_rdf.py convert --input {} --output {} --schema {} {}'.format(comment, script_dir, path, output_path, schema_path, limit)


def cmd_gen(manifest, cmd_outdir, rdf_outdir, limit):
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
    # write schema files for each type
    py2dgraph = {
        'str': 'string',
    }
    for label in headers.keys():
        output_path = os.path.join(rdf_outdir, '{}.schema.rdf'.format(label))
        with open(output_path, "w", newline='') as myfile:
            for k, v in headers[label].items():
                f, t = v.split(":")
                if f in ["gid", "label", "from", "to"]:
                    continue
                myfile.write('<{}>: {} .\n'.format(f, py2dgraph.get(t, t)))

    for path in config.vertex_files:
        if not os.path.isfile(path):
            logging.warning('{} does not exist'.format(path))
            continue
        label = get_label(path)
        if label not in vertex_rdfs:
            vertex_rdfs[label] = []
            vertex_rdfs[label].append(os.path.join(rdf_outdir, '{}.Vertex.schema.rdf'.format(label)))
        to_rdf_commands.append(to_rdf_job(path, rdf_outdir, limit=limit))
        vertex_rdfs[label].append(get_output_path(rdf_outdir, path))

    for path in config.edge_files:
        if not os.path.isfile(path):
            logging.warning('{} does not exist'.format(path))
            continue
        label = get_label(path)
        if label not in edge_rdfs:
            edge_rdfs[label] = []
            edge_rdfs[label].append(os.path.join(rdf_outdir, '{}.Edge.schema.rdf'.format(label)))
        to_rdf_commands.append(to_rdf_job(path, rdf_outdir, limit=limit))
        edge_rdfs[label].append(get_output_path(rdf_outdir, path))

    to_rdf_path = os.path.join(cmd_outdir, 'to_rdf_commands.txt')
    with open(to_rdf_path, 'w') as outfile:
        for command in to_rdf_commands:
            outfile.write('{}\n'.format(command))
    logging.info('wrote {}'.format(to_rdf_path))

    load_path = os.path.join(cmd_outdir, 'load_db.txt')
    with open(load_path, 'w') as outfile:
        outfile.write('parallel --jobs {} < {}\n'.format(multiprocessing.cpu_count(), to_rdf_path))
        outfile.write('cat {} | sort | uniq > {}\n'.format(os.path.join(rdf_outdir, '*.schema.rdf'), os.path.join(rdf_outdir, 'schema.rdf')))
        outfile.write('cat {} > {}\n'.format(os.path.join(rdf_outdir, '*.json.gz.rdf'), os.path.join(rdf_outdir, 'data.rdf')))
        # TODO
        # outfile.write('dgraph bulk --schema {}/schema.rdf --rdfs {}/data.rdf --out {}'.format(rdf_outdir, rdf_outdir, dgraph_alpha_dir))
    logging.info('wrote {}'.format(load_path))


if __name__ == '__main__':  # pragma: no cover
    logging.getLogger().setLevel(logging.DEBUG)
    parser = argparse.ArgumentParser(description='Loads vertexes and edges into dgraph')
    subparsers = parser.add_subparsers(help='sub-command help')
    cmdgen_parser = subparsers.add_parser('cmd-gen', help='generate to_rdf commands')
    cmdgen_parser.add_argument('-l', '--limit', dest='limit', type=int, default=None, help='limit the number of rows in each vertex/edge')
    cmdgen_parser.add_argument('-m', '--manifest', dest='manifest', required=True, help='manifest file path')
    cmdgen_parser.add_argument('-c', '--cmd-outdir', dest='cmd_outdir', default='.', help='directory in which to write command files (to_rdf_commands.txt, load_db.txt)')
    cmdgen_parser.add_argument('-r', '--rdf-outdir', dest='rdf_outdir', default='.', help='directory in which commands should specify to write rdf files')
    cmdgen_parser.set_defaults(func=cmd_gen)
    tordf_parser = subparsers.add_parser('convert', help='convert input json to RDF')
    tordf_parser.add_argument('-l', '--limit', dest='limit', type=int, default=None, help='limit the number of rows in each vertex/edge')
    tordf_parser.add_argument('-i', '--input', dest='input', required=True, help='path for single input file')
    tordf_parser.add_argument('-o', '--output', dest='output', required=True, help='path for single output file')
    tordf_parser.add_argument('-s', '--schema', dest='schema', required=True, help='path to corresponding schema file')
    tordf_parser.set_defaults(func=to_rdf)
    args = parser.parse_args()
    logging.debug(vars(args))
    cmd_args = vars(args).copy()
    if 'func' not in cmd_args:
        parser.print_help()
        sys.exit(0)
    del cmd_args['func']
    args.func(**cmd_args)
