// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

use crate::db::TypedSQLValue;
use db_traits::errors::{DbErrorKind, Result};
use edn;
use edn::symbols;

use core_traits::{attribute, Attribute, Entid, KnownEntid, TypedValue, ValueType};

use crate::metadata;
use crate::metadata::AttributeAlteration;
use mentat_core::{AttributeMap, EntidMap, HasSchema, IdentMap, Schema};

pub trait AttributeValidation {
    fn validate<F>(&self, ident: F) -> Result<()>
    where
        F: Fn() -> String;
}

impl AttributeValidation for Attribute {
    fn validate<F>(&self, ident: F) -> Result<()>
    where
        F: Fn() -> String,
    {
        if self.unique == Some(attribute::Unique::Value) && !self.index {
            bail!(DbErrorKind::BadSchemaAssertion(format!(
                ":db/unique :db/unique_value without :db/index true for entid: {}",
                ident()
            )))
        }
        if self.unique == Some(attribute::Unique::Identity) && !self.index {
            bail!(DbErrorKind::BadSchemaAssertion(format!(
                ":db/unique :db/unique_identity without :db/index true for entid: {}",
                ident()
            )))
        }
        if self.fulltext && self.value_type != ValueType::String {
            bail!(DbErrorKind::BadSchemaAssertion(format!(
                ":db/fulltext true without :db/valueType :db.type/string for entid: {}",
                ident()
            )))
        }
        if self.fulltext && !self.index {
            bail!(DbErrorKind::BadSchemaAssertion(format!(
                ":db/fulltext true without :db/index true for entid: {}",
                ident()
            )))
        }
        if self.component && self.value_type != ValueType::Ref {
            bail!(DbErrorKind::BadSchemaAssertion(format!(
                ":db/isComponent true without :db/valueType :db.type/ref for entid: {}",
                ident()
            )))
        }
        // TODO: consider warning if we have :db/index true for :db/valueType :db.type/string,
        // since this may be inefficient.  More generally, we should try to drive complex
        // :db/valueType (string, uri, json in the future) users to opt-in to some hash-indexing
        // scheme, as discussed in https://github.com/mozilla/mentat/issues/69.
        Ok(())
    }
}

/// Return `Ok(())` if `attribute_map` defines a valid Mentat schema.
fn validate_attribute_map(entid_map: &EntidMap, attribute_map: &AttributeMap) -> Result<()> {
    for (entid, attribute) in attribute_map {
        let ident = || {
            entid_map
                .get(entid)
                .map(|ident| ident.to_string())
                .unwrap_or_else(|| entid.to_string())
        };
        attribute.validate(ident)?;
    }
    Ok(())
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct AttributeBuilder {
    helpful: bool,
    pub value_type: Option<ValueType>,
    pub multival: Option<bool>,
    pub unique: Option<Option<attribute::Unique>>,
    pub index: Option<bool>,
    pub fulltext: Option<bool>,
    pub component: Option<bool>,
    pub no_history: Option<bool>,
}

impl AttributeBuilder {
    /// Make a new AttributeBuilder for human consumption: it will help you
    /// by flipping relevant flags.
    pub fn helpful() -> Self {
        AttributeBuilder {
            helpful: true,
            ..Default::default()
        }
    }

    /// Make a new AttributeBuilder from an existing Attribute. This is important to allow
    /// retraction. Only attributes that we allow to change are duplicated here.
    pub fn modify_attribute(attribute: &Attribute) -> Self {
        let mut ab = AttributeBuilder::default();
        ab.multival = Some(attribute.multival);
        ab.unique = Some(attribute.unique);
        ab.component = Some(attribute.component);
        ab
    }

    pub fn value_type(&mut self, value_type: ValueType) -> &mut Self {
        self.value_type = Some(value_type);
        self
    }

    pub fn multival(&mut self, multival: bool) -> &mut Self {
        self.multival = Some(multival);
        self
    }

    pub fn non_unique(&mut self) -> &mut Self {
        self.unique = Some(None);
        self
    }

    pub fn unique(&mut self, unique: attribute::Unique) -> &mut Self {
        if self.helpful && unique == attribute::Unique::Identity {
            self.index = Some(true);
        }
        self.unique = Some(Some(unique));
        self
    }

    pub fn index(&mut self, index: bool) -> &mut Self {
        self.index = Some(index);
        self
    }

    pub fn fulltext(&mut self, fulltext: bool) -> &mut Self {
        self.fulltext = Some(fulltext);
        if self.helpful && fulltext {
            self.index = Some(true);
        }
        self
    }

    pub fn component(&mut self, component: bool) -> &mut Self {
        self.component = Some(component);
        self
    }

    pub fn no_history(&mut self, no_history: bool) -> &mut Self {
        self.no_history = Some(no_history);
        self
    }

    pub fn validate_install_attribute(&self) -> Result<()> {
        if self.value_type.is_none() {
            bail!(DbErrorKind::BadSchemaAssertion(
                "Schema attribute for new attribute does not set :db/valueType".into()
            ));
        }
        Ok(())
    }

    pub fn validate_alter_attribute(&self) -> Result<()> {
        if self.value_type.is_some() {
            bail!(DbErrorKind::BadSchemaAssertion(
                "Schema alteration must not set :db/valueType".into()
            ));
        }
        if self.fulltext.is_some() {
            bail!(DbErrorKind::BadSchemaAssertion(
                "Schema alteration must not set :db/fulltext".into()
            ));
        }
        Ok(())
    }

    pub fn build(&self) -> Attribute {
        let mut attribute = Attribute::default();
        if let Some(value_type) = self.value_type {
            attribute.value_type = value_type;
        }
        if let Some(fulltext) = self.fulltext {
            attribute.fulltext = fulltext;
        }
        if let Some(multival) = self.multival {
            attribute.multival = multival;
        }
        if let Some(ref unique) = self.unique {
            attribute.unique = *unique;
        }
        if let Some(index) = self.index {
            attribute.index = index;
        }
        if let Some(component) = self.component {
            attribute.component = component;
        }
        if let Some(no_history) = self.no_history {
            attribute.no_history = no_history;
        }

        attribute
    }

    pub fn mutate(&self, attribute: &mut Attribute) -> Vec<AttributeAlteration> {
        let mut mutations = Vec::new();
        if let Some(multival) = self.multival {
            if multival != attribute.multival {
                attribute.multival = multival;
                mutations.push(AttributeAlteration::Cardinality);
            }
        }

        if let Some(ref unique) = self.unique {
            if *unique != attribute.unique {
                attribute.unique = *unique;
                mutations.push(AttributeAlteration::Unique);
            }
        } else if attribute.unique != None {
            attribute.unique = None;
            mutations.push(AttributeAlteration::Unique);
        }

        if let Some(index) = self.index {
            if index != attribute.index {
                attribute.index = index;
                mutations.push(AttributeAlteration::Index);
            }
        }
        if let Some(component) = self.component {
            if component != attribute.component {
                attribute.component = component;
                mutations.push(AttributeAlteration::IsComponent);
            }
        }
        if let Some(no_history) = self.no_history {
            if no_history != attribute.no_history {
                attribute.no_history = no_history;
                mutations.push(AttributeAlteration::NoHistory);
            }
        }

        mutations
    }
}

pub trait SchemaBuilding {
    fn require_ident(&self, entid: Entid) -> Result<&symbols::Keyword>;
    fn require_entid(&self, ident: &symbols::Keyword) -> Result<KnownEntid>;
    fn require_attribute_for_entid(&self, entid: Entid) -> Result<&Attribute>;
    fn from_ident_map_and_attribute_map(
        ident_map: IdentMap,
        attribute_map: AttributeMap,
    ) -> Result<Schema>;
    fn from_ident_map_and_triples<U>(ident_map: IdentMap, assertions: U) -> Result<Schema>
    where
        U: IntoIterator<Item = (symbols::Keyword, symbols::Keyword, TypedValue)>;
}

impl SchemaBuilding for Schema {
    fn require_ident(&self, entid: Entid) -> Result<&symbols::Keyword> {
        self.get_ident(entid)
            .ok_or_else(|| DbErrorKind::UnrecognizedEntid(entid).into())
    }

    fn require_entid(&self, ident: &symbols::Keyword) -> Result<KnownEntid> {
        self.get_entid(&ident)
            .ok_or_else(|| DbErrorKind::UnrecognizedIdent(ident.to_string()).into())
    }

    fn require_attribute_for_entid(&self, entid: Entid) -> Result<&Attribute> {
        self.attribute_for_entid(entid)
            .ok_or_else(|| DbErrorKind::UnrecognizedEntid(entid).into())
    }

    /// Create a valid `Schema` from the constituent maps.
    fn from_ident_map_and_attribute_map(
        ident_map: IdentMap,
        attribute_map: AttributeMap,
    ) -> Result<Schema> {
        let entid_map: EntidMap = ident_map.iter().map(|(k, v)| (*v, k.clone())).collect();

        validate_attribute_map(&entid_map, &attribute_map)?;
        Ok(Schema::new(ident_map, entid_map, attribute_map))
    }

    /// Turn vec![(Keyword(:ident), Keyword(:key), TypedValue(:value)), ...] into a Mentat `Schema`.
    fn from_ident_map_and_triples<U>(ident_map: IdentMap, assertions: U) -> Result<Schema>
    where
        U: IntoIterator<Item = (symbols::Keyword, symbols::Keyword, TypedValue)>,
    {
        let entid_assertions: Result<Vec<(Entid, Entid, TypedValue)>> = assertions
            .into_iter()
            .map(|(symbolic_ident, symbolic_attr, value)| {
                let ident: i64 = *ident_map
                    .get(&symbolic_ident)
                    .ok_or_else(|| DbErrorKind::UnrecognizedIdent(symbolic_ident.to_string()))?;
                let attr: i64 = *ident_map
                    .get(&symbolic_attr)
                    .ok_or_else(|| DbErrorKind::UnrecognizedIdent(symbolic_attr.to_string()))?;
                Ok((ident, attr, value))
            })
            .collect();

        let mut schema =
            Schema::from_ident_map_and_attribute_map(ident_map, AttributeMap::default())?;
        let metadata_report = metadata::update_attribute_map_from_entid_triples(
            &mut schema.attribute_map,
            entid_assertions?,
            // No retractions.
            vec![],
        )?;

        // Rebuild the component attributes list if necessary.
        if metadata_report.attributes_did_change() {
            schema.update_component_attributes();
        }
        Ok(schema)
    }
}

pub trait SchemaTypeChecking {
    /// Do schema-aware typechecking and coercion.
    ///
    /// Either assert that the given value is in the value type's value set, or (in limited cases)
    /// coerce the given value into the value type's value set.
    fn to_typed_value(
        &self,
        value: &edn::ValueAndSpan,
        value_type: ValueType,
    ) -> Result<TypedValue>;
}

impl SchemaTypeChecking for Schema {
    fn to_typed_value(
        &self,
        value: &edn::ValueAndSpan,
        value_type: ValueType,
    ) -> Result<TypedValue> {
        // TODO: encapsulate entid-ident-attribute for better error messages, perhaps by including
        // the attribute (rather than just the attribute's value type) into this function or a
        // wrapper function.
        match TypedValue::from_edn_value(&value.clone().without_spans()) {
            // We don't recognize this EDN at all.  Get out!
            None => bail!(DbErrorKind::BadValuePair(format!("{}", value), value_type)),
            Some(typed_value) => match (value_type, typed_value) {
                // Most types don't coerce at all.
                (ValueType::Boolean, tv @ TypedValue::Boolean(_)) => Ok(tv),
                (ValueType::Long, tv @ TypedValue::Long(_)) => Ok(tv),
                (ValueType::Double, tv @ TypedValue::Double(_)) => Ok(tv),
                (ValueType::String, tv @ TypedValue::String(_)) => Ok(tv),
                (ValueType::Uuid, tv @ TypedValue::Uuid(_)) => Ok(tv),
                (ValueType::Instant, tv @ TypedValue::Instant(_)) => Ok(tv),
                (ValueType::Keyword, tv @ TypedValue::Keyword(_)) => Ok(tv),
                (ValueType::Bytes, tv @ TypedValue::Bytes(_)) => Ok(tv),
                // Ref coerces a little: we interpret some things depending on the schema as a Ref.
                (ValueType::Ref, TypedValue::Long(x)) => Ok(TypedValue::Ref(x)),
                (ValueType::Ref, TypedValue::Keyword(ref x)) => {
                    self.require_entid(&x).map(|entid| entid.into())
                }

                // Otherwise, we have a type mismatch.
                // Enumerate all of the types here to allow the compiler to help us.
                // We don't enumerate all `TypedValue` cases, though: that would multiply this
                // collection by 8!
                (vt @ ValueType::Boolean, _)
                | (vt @ ValueType::Long, _)
                | (vt @ ValueType::Double, _)
                | (vt @ ValueType::String, _)
                | (vt @ ValueType::Uuid, _)
                | (vt @ ValueType::Instant, _)
                | (vt @ ValueType::Keyword, _)
                | (vt @ ValueType::Bytes, _)
                | (vt @ ValueType::Ref, _) => {
                    bail!(DbErrorKind::BadValuePair(format!("{}", value), vt))
                }
            },
        }
    }
}

#[cfg(test)]
mod test {
    use self::edn::Keyword;
    use super::*;

    fn add_attribute(schema: &mut Schema, ident: Keyword, entid: Entid, attribute: Attribute) {
        schema.entid_map.insert(entid, ident.clone());
        schema.ident_map.insert(ident, entid);

        if attribute.component {
            schema.component_attributes.push(entid);
        }

        schema.attribute_map.insert(entid, attribute);
    }

    #[test]
    fn validate_attribute_map_success() {
        let mut schema = Schema::default();
        // attribute that is not an index has no uniqueness
        add_attribute(
            &mut schema,
            Keyword::namespaced("foo", "bar"),
            97,
            Attribute {
                index: false,
                value_type: ValueType::Boolean,
                fulltext: false,
                unique: None,
                multival: false,
                component: false,
                no_history: false,
            },
        );
        // attribute is unique by value and an index
        add_attribute(
            &mut schema,
            Keyword::namespaced("foo", "baz"),
            98,
            Attribute {
                index: true,
                value_type: ValueType::Long,
                fulltext: false,
                unique: Some(attribute::Unique::Value),
                multival: false,
                component: false,
                no_history: false,
            },
        );
        // attribue is unique by identity and an index
        add_attribute(
            &mut schema,
            Keyword::namespaced("foo", "bat"),
            99,
            Attribute {
                index: true,
                value_type: ValueType::Ref,
                fulltext: false,
                unique: Some(attribute::Unique::Identity),
                multival: false,
                component: false,
                no_history: false,
            },
        );
        // attribute is a components and a `Ref`
        add_attribute(
            &mut schema,
            Keyword::namespaced("foo", "bak"),
            100,
            Attribute {
                index: false,
                value_type: ValueType::Ref,
                fulltext: false,
                unique: None,
                multival: false,
                component: true,
                no_history: false,
            },
        );
        // fulltext attribute is a string and an index
        add_attribute(
            &mut schema,
            Keyword::namespaced("foo", "bap"),
            101,
            Attribute {
                index: true,
                value_type: ValueType::String,
                fulltext: true,
                unique: None,
                multival: false,
                component: false,
                no_history: false,
            },
        );

        assert!(validate_attribute_map(&schema.entid_map, &schema.attribute_map).is_ok());
    }

    #[test]
    fn invalid_schema_unique_value_not_index() {
        let mut schema = Schema::default();
        // attribute unique by value but not index
        let ident = Keyword::namespaced("foo", "bar");
        add_attribute(
            &mut schema,
            ident,
            99,
            Attribute {
                index: false,
                value_type: ValueType::Boolean,
                fulltext: false,
                unique: Some(attribute::Unique::Value),
                multival: false,
                component: false,
                no_history: false,
            },
        );

        let err = validate_attribute_map(&schema.entid_map, &schema.attribute_map)
            .err()
            .map(|e| e.kind());
        assert_eq!(
            err,
            Some(DbErrorKind::BadSchemaAssertion(
                ":db/unique :db/unique_value without :db/index true for entid: :foo/bar".into()
            ))
        );
    }

    #[test]
    fn invalid_schema_unique_identity_not_index() {
        let mut schema = Schema::default();
        // attribute is unique by identity but not index
        add_attribute(
            &mut schema,
            Keyword::namespaced("foo", "bar"),
            99,
            Attribute {
                index: false,
                value_type: ValueType::Long,
                fulltext: false,
                unique: Some(attribute::Unique::Identity),
                multival: false,
                component: false,
                no_history: false,
            },
        );

        let err = validate_attribute_map(&schema.entid_map, &schema.attribute_map)
            .err()
            .map(|e| e.kind());
        assert_eq!(
            err,
            Some(DbErrorKind::BadSchemaAssertion(
                ":db/unique :db/unique_identity without :db/index true for entid: :foo/bar".into()
            ))
        );
    }

    #[test]
    fn invalid_schema_component_not_ref() {
        let mut schema = Schema::default();
        // attribute that is a component is not a `Ref`
        add_attribute(
            &mut schema,
            Keyword::namespaced("foo", "bar"),
            99,
            Attribute {
                index: false,
                value_type: ValueType::Boolean,
                fulltext: false,
                unique: None,
                multival: false,
                component: true,
                no_history: false,
            },
        );

        let err = validate_attribute_map(&schema.entid_map, &schema.attribute_map)
            .err()
            .map(|e| e.kind());
        assert_eq!(
            err,
            Some(DbErrorKind::BadSchemaAssertion(
                ":db/isComponent true without :db/valueType :db.type/ref for entid: :foo/bar"
                    .into()
            ))
        );
    }

    #[test]
    fn invalid_schema_fulltext_not_index() {
        let mut schema = Schema::default();
        // attribute that is fulltext is not an index
        add_attribute(
            &mut schema,
            Keyword::namespaced("foo", "bar"),
            99,
            Attribute {
                index: false,
                value_type: ValueType::String,
                fulltext: true,
                unique: None,
                multival: false,
                component: false,
                no_history: false,
            },
        );

        let err = validate_attribute_map(&schema.entid_map, &schema.attribute_map)
            .err()
            .map(|e| e.kind());
        assert_eq!(
            err,
            Some(DbErrorKind::BadSchemaAssertion(
                ":db/fulltext true without :db/index true for entid: :foo/bar".into()
            ))
        );
    }

    fn invalid_schema_fulltext_index_not_string() {
        let mut schema = Schema::default();
        // attribute that is fulltext and not a `String`
        add_attribute(
            &mut schema,
            Keyword::namespaced("foo", "bar"),
            99,
            Attribute {
                index: true,
                value_type: ValueType::Long,
                fulltext: true,
                unique: None,
                multival: false,
                component: false,
                no_history: false,
            },
        );

        let err = validate_attribute_map(&schema.entid_map, &schema.attribute_map)
            .err()
            .map(|e| e.kind());
        assert_eq!(
            err,
            Some(DbErrorKind::BadSchemaAssertion(
                ":db/fulltext true without :db/valueType :db.type/string for entid: :foo/bar"
                    .into()
            ))
        );
    }
}
