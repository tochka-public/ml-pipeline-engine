export enum NodeType {
  datasource = 'datasource',
  feature = 'feature',
  ml_model = 'ml_model',
  processor = 'processor',
  switch = 'switch',
  map = 'map',
  reduce = 'reduce',
  other = 'other',
}

export enum DisplayNameMode {
  id = 'id',
  name = 'name',
  verbose_name = 'verbose_name',
}

export enum Highlight {
  selected = 'selected',
  predcessor = 'predcessor',
  successor = 'successor',
}