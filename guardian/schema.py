"""Schema definition, evolution rules and type-drift helpers.

Types are canonical strings: int, long, float, double, string, bool,
timestamp, date. Widening (int->long, float->double, int->double ...) is
allowed; narrowing or unrelated changes are rejected.
"""
from __future__ import annotations
from dataclasses import dataclass, field, asdict

WIDENING = {
    "int": {"long", "double"},
    "long": {"double"},
    "float": {"double"},
    "date": {"timestamp"},
}

# pyarrow type -> canonical
def canonical_type(pa_type) -> str:
    s = str(pa_type).lower()
    if s.startswith("timestamp"): return "timestamp"
    if s.startswith("date"): return "date"
    return {"int32":"int","int64":"long","float":"float","double":"double",
            "string":"string","large_string":"string","bool":"bool",
            "boolean":"bool"}.get(s, s)

@dataclass
class Field:
    name: str
    type: str
    nullable: bool = True
    default: object = None
    def to_dict(self): return asdict(self)

@dataclass
class Schema:
    fields: list[Field] = field(default_factory=list)
    schema_id: int = 0
    def names(self): return [f.name for f in self.fields]
    def get(self, name):
        for f in self.fields:
            if f.name == name: return f
        return None
    def to_dict(self): return {"schema_id": self.schema_id, "fields": [f.to_dict() for f in self.fields]}
    @classmethod
    def from_dict(cls, d):
        return cls([Field(**f) for f in d.get("fields", [])], d.get("schema_id", 0))
    @classmethod
    def from_json_obj(cls, obj):
        """Accept [{'name':..,'type':..}, ...] or {'fields':[...]}."""
        if isinstance(obj, dict): obj = obj.get("fields", [])
        return cls([Field(f["name"], f["type"], f.get("nullable", True), f.get("default")) for f in obj])

class SchemaEvolutionError(ValueError): pass

def evolve_schema(schema: Schema, ops: list[dict]) -> Schema:
    """Apply ops: {op:add,name,type,nullable,default} | {op:drop,name} |
    {op:rename,from,to} | {op:widen,name,type} | {op:require,name}/{op:make_nullable,name}.
    Returns a new Schema with schema_id+1; raises on unsafe changes."""
    fields = [Field(f.name, f.type, f.nullable, f.default) for f in schema.fields]
    def idx(name):
        for i, f in enumerate(fields):
            if f.name == name: return i
        return -1
    for op in ops:
        kind = op.get("op")
        if kind == "add":
            if idx(op["name"]) >= 0: raise SchemaEvolutionError(f"column already exists: {op['name']}")
            if not op.get("nullable", True) and op.get("default") is None:
                raise SchemaEvolutionError("adding a non-nullable column requires a default")
            fields.append(Field(op["name"], op["type"], op.get("nullable", True), op.get("default")))
        elif kind == "drop":
            i = idx(op["name"])
            if i < 0: raise SchemaEvolutionError(f"no such column: {op['name']}")
            fields.pop(i)
        elif kind == "rename":
            i = idx(op["from"])
            if i < 0: raise SchemaEvolutionError(f"no such column: {op['from']}")
            if idx(op["to"]) >= 0: raise SchemaEvolutionError(f"target exists: {op['to']}")
            fields[i].name = op["to"]
        elif kind in ("widen", "retype"):
            i = idx(op["name"])
            if i < 0: raise SchemaEvolutionError(f"no such column: {op['name']}")
            new = op["type"]
            if new != fields[i].type and new not in WIDENING.get(fields[i].type, set()):
                raise SchemaEvolutionError(f"unsafe type change {fields[i].type} -> {new} for {op['name']}")
            fields[i].type = new
        elif kind == "make_nullable":
            i = idx(op["name"]);  fields[i].nullable = True
        elif kind == "require":
            i = idx(op["name"])
            if i < 0: raise SchemaEvolutionError(f"no such column: {op['name']}")
            fields[i].nullable = False
        else:
            raise SchemaEvolutionError(f"unknown op: {kind}")
    return Schema(fields, schema.schema_id + 1)

def type_is_compatible(expected: str, actual: str) -> bool:
    return expected == actual or expected in WIDENING.get(actual, set()) or actual in WIDENING.get(expected, set())
