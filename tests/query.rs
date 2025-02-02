// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#[macro_use]
extern crate mentat;

// TODO: when we switch to `failure`, make this more humane.
//use query_algebrizer_traits; // For errors;

use std::str::FromStr;

use chrono::FixedOffset;

use core_traits::{Entid, KnownEntid, ValueType, ValueTypeSet};

use edn::OrderedFloat;
use mentat_core::{DateTime, HasSchema, Utc, Uuid};

use query_projector_traits::aggregates::SimpleAggregationOp;

use mentat::{
    new_connection, Binding, IntoResult, Keyword, PlainSymbol, QueryInputs, QueryResults,
    Queryable, RelResult, Store, TxReport, TypedValue, Variable,
};

use mentat::query::q_uncached;

use mentat::conn::Conn;

use public_traits::errors::MentatError;

use assert_approx_eq::assert_approx_eq;

#[test]
fn test_rel() {
    let mut c = new_connection("").expect("Couldn't open conn.");
    let db = mentat_db::db::ensure_current_version(&mut c).expect("Couldn't open DB.");

    // Rel.
    let start = time::Instant::now();
    let results = q_uncached(
        &c,
        &db.schema,
        "[:find ?x ?ident :where [?x :db/ident ?ident]]",
        None,
    )
    .expect("Query failed")
    .results;
    let end = time::Instant::now();

    // This will need to change each time we add a default ident.
    assert_eq!(40, results.len());

    // Every row is a pair of a Ref and a Keyword.
    if let QueryResults::Rel(rel) = results {
        for r in rel.into_iter() {
            assert_eq!(r.len(), 2);
            assert!(r[0].matches_type(ValueType::Ref));
            assert!(r[1].matches_type(ValueType::Keyword));
        }
    } else {
        panic!("Expected rel.");
    }

    println!("Rel took {}µs", (end - start).whole_microseconds());
}

#[test]
fn test_failing_scalar() {
    let mut c = new_connection("").expect("Couldn't open conn.");
    let db = mentat_db::db::ensure_current_version(&mut c).expect("Couldn't open DB.");

    // Scalar that fails.
    let start = time::Instant::now();
    let results = q_uncached(
        &c,
        &db.schema,
        "[:find ?x . :where [?x :db/fulltext true]]",
        None,
    )
    .expect("Query failed")
    .results;
    let end = time::Instant::now();

    assert_eq!(0, results.len());

    if let QueryResults::Scalar(None) = results {
    } else {
        panic!("Expected failed scalar.");
    }

    println!(
        "Failing scalar took {}µs",
        (end - start).whole_microseconds()
    );
}

#[test]
fn test_scalar() {
    let mut c = new_connection("").expect("Couldn't open conn.");
    let db = mentat_db::db::ensure_current_version(&mut c).expect("Couldn't open DB.");

    // Scalar that succeeds.
    let start = time::Instant::now();
    let results = q_uncached(
        &c,
        &db.schema,
        "[:find ?ident . :where [24 :db/ident ?ident]]",
        None,
    )
    .expect("Query failed")
    .results;
    let end = time::Instant::now();

    assert_eq!(1, results.len());

    if let QueryResults::Scalar(Some(Binding::Scalar(TypedValue::Keyword(ref rc)))) = results {
        // Should be '24'.
        assert_eq!(&Keyword::namespaced("db.type", "keyword"), rc.as_ref());
        assert_eq!(KnownEntid(24), db.schema.get_entid(rc).unwrap());
    } else {
        panic!("Expected scalar.");
    }

    println!("{:?}", results);
    println!("Scalar took {}µs", (end - start).whole_microseconds());
}

#[test]
fn test_tuple() {
    let mut c = new_connection("").expect("Couldn't open conn.");
    let db = mentat_db::db::ensure_current_version(&mut c).expect("Couldn't open DB.");

    // Tuple.
    let start = time::Instant::now();
    let results = q_uncached(
        &c,
        &db.schema,
        "[:find [?index ?cardinality]
                               :where [:db/txInstant :db/index ?index]
                                      [:db/txInstant :db/cardinality ?cardinality]]",
        None,
    )
    .expect("Query failed")
    .results;
    let end = time::Instant::now();

    assert_eq!(1, results.len());

    if let QueryResults::Tuple(Some(ref tuple)) = results {
        let cardinality_one = Keyword::namespaced("db.cardinality", "one");
        assert_eq!(tuple.len(), 2);
        assert_eq!(tuple[0], TypedValue::Boolean(true).into());
        assert_eq!(
            tuple[1],
            db.schema.get_entid(&cardinality_one).expect("c1").into()
        );
    } else {
        panic!("Expected tuple.");
    }

    println!("{:?}", results);
    println!("Tuple took {}µs", (end - start).whole_microseconds());
}

#[test]
fn test_coll() {
    let mut c = new_connection("").expect("Couldn't open conn.");
    let db = mentat_db::db::ensure_current_version(&mut c).expect("Couldn't open DB.");

    // Coll.
    let start = time::Instant::now();
    let results = q_uncached(
        &c,
        &db.schema,
        "[:find [?e ...] :where [?e :db/ident _]]",
        None,
    )
    .expect("Query failed")
    .results;
    let end = time::Instant::now();

    assert_eq!(40, results.len());

    if let QueryResults::Coll(ref coll) = results {
        assert!(coll.iter().all(|item| item.matches_type(ValueType::Ref)));
    } else {
        panic!("Expected coll.");
    }

    println!("{:?}", results);
    println!("Coll took {}µs", (end - start).whole_microseconds());
}

#[test]
fn test_inputs() {
    let mut c = new_connection("").expect("Couldn't open conn.");
    let db = mentat_db::db::ensure_current_version(&mut c).expect("Couldn't open DB.");

    // entids::DB_INSTALL_VALUE_TYPE = 5.
    let ee = (Variable::from_valid_name("?e"), TypedValue::Ref(5));
    let inputs = QueryInputs::with_value_sequence(vec![ee]);
    let results = q_uncached(
        &c,
        &db.schema,
        "[:find ?i . :in ?e :where [?e :db/ident ?i]]",
        inputs,
    )
    .expect("query to succeed")
    .results;

    if let QueryResults::Scalar(Some(Binding::Scalar(TypedValue::Keyword(value)))) = results {
        assert_eq!(
            value.as_ref(),
            &Keyword::namespaced("db.install", "valueType")
        );
    } else {
        panic!("Expected scalar.");
    }
}

/// Ensure that a query won't be run without all of its `:in` variables being bound.
#[test]
fn test_unbound_inputs() {
    let mut c = new_connection("").expect("Couldn't open conn.");
    let db = mentat_db::db::ensure_current_version(&mut c).expect("Couldn't open DB.");

    // Bind the wrong var by 'mistake'.
    let xx = (Variable::from_valid_name("?x"), TypedValue::Ref(5));
    let inputs = QueryInputs::with_value_sequence(vec![xx]);
    let results = q_uncached(
        &c,
        &db.schema,
        "[:find ?i . :in ?e :where [?e :db/ident ?i]]",
        inputs,
    );

    match results.expect_err("expected unbound variables") {
        MentatError::UnboundVariables(vars) => {
            assert_eq!(vars, vec!["?e".to_string()].into_iter().collect());
        }
        _ => panic!("Expected UnboundVariables variant."),
    }
}

#[test]
fn test_instants_and_uuids() {
    // We assume, perhaps foolishly, that the clocks on test machines won't lose more than an
    // hour while this test is running.
    let start = Utc::now() + FixedOffset::west(60 * 60);

    let mut c = new_connection("").expect("Couldn't open conn.");
    let mut conn = Conn::connect(&mut c).expect("Couldn't open DB.");
    conn.transact(
        &mut c,
        r#"[
        [:db/add "s" :db/ident :foo/uuid]
        [:db/add "s" :db/valueType :db.type/uuid]
        [:db/add "s" :db/cardinality :db.cardinality/one]
    ]"#,
    )
    .unwrap();
    conn.transact(
        &mut c,
        r#"[
        [:db/add "u" :foo/uuid #uuid "cf62d552-6569-4d1b-b667-04703041dfc4"]
    ]"#,
    )
    .unwrap();
    let r = conn
        .q_once(
            &c,
            r#"[:find [?x ?u ?when]
                            :where [?x :foo/uuid ?u ?tx]
                                   [?tx :db/txInstant ?when]]"#,
            None,
        )
        .expect("results")
        .into();
    match r {
        QueryResults::Tuple(Some(vals)) => {
            let mut vals = vals.into_iter();
            match (vals.next(), vals.next(), vals.next(), vals.next()) {
                (
                    Some(Binding::Scalar(TypedValue::Ref(e))),
                    Some(Binding::Scalar(TypedValue::Uuid(u))),
                    Some(Binding::Scalar(TypedValue::Instant(t))),
                    None,
                ) => {
                    assert!(e > 40); // There are at least this many entities in the store.
                    assert_eq!(
                        Ok(u),
                        Uuid::from_str("cf62d552-6569-4d1b-b667-04703041dfc4")
                    );
                    assert!(t > start);
                }
                _ => panic!("Unexpected results."),
            }
        }
        _ => panic!("Expected query to work."),
    }
}

#[test]
fn test_tx() {
    let mut c = new_connection("").expect("Couldn't open conn.");
    let mut conn = Conn::connect(&mut c).expect("Couldn't open DB.");
    conn.transact(
        &mut c,
        r#"[
        [:db/add "s" :db/ident :foo/uuid]
        [:db/add "s" :db/valueType :db.type/uuid]
        [:db/add "s" :db/cardinality :db.cardinality/one]
    ]"#,
    )
    .expect("successful transaction");

    let t = conn
        .transact(
            &mut c,
            r#"[
        [:db/add "u" :foo/uuid #uuid "cf62d552-6569-4d1b-b667-04703041dfc4"]
    ]"#,
        )
        .expect("successful transaction");

    conn.transact(
        &mut c,
        r#"[
        [:db/add "u" :foo/uuid #uuid "550e8400-e29b-41d4-a716-446655440000"]
    ]"#,
    )
    .expect("successful transaction");

    let r = conn.q_once(&c,
                        r#"[:find ?tx
                            :where [?x :foo/uuid #uuid "cf62d552-6569-4d1b-b667-04703041dfc4" ?tx]]"#, None)
                .expect("results")
                .into();
    match r {
        QueryResults::Rel(ref v) => {
            assert_eq!(*v, vec![vec![TypedValue::Ref(t.tx_id),]].into());
        }
        _ => panic!("Expected query to work."),
    }
}

#[test]
fn test_tx_as_input() {
    let mut c = new_connection("").expect("Couldn't open conn.");
    let mut conn = Conn::connect(&mut c).expect("Couldn't open DB.");
    conn.transact(
        &mut c,
        r#"[
        [:db/add "s" :db/ident :foo/uuid]
        [:db/add "s" :db/valueType :db.type/uuid]
        [:db/add "s" :db/cardinality :db.cardinality/one]
    ]"#,
    )
    .expect("successful transaction");
    conn.transact(
        &mut c,
        r#"[
        [:db/add "u" :foo/uuid #uuid "550e8400-e29b-41d4-a716-446655440000"]
    ]"#,
    )
    .expect("successful transaction");
    let t = conn
        .transact(
            &mut c,
            r#"[
        [:db/add "u" :foo/uuid #uuid "cf62d552-6569-4d1b-b667-04703041dfc4"]
    ]"#,
        )
        .expect("successful transaction");
    conn.transact(
        &mut c,
        r#"[
        [:db/add "u" :foo/uuid #uuid "267bab92-ee39-4ca2-b7f0-1163a85af1fb"]
    ]"#,
    )
    .expect("successful transaction");

    let tx = (Variable::from_valid_name("?tx"), TypedValue::Ref(t.tx_id));
    let inputs = QueryInputs::with_value_sequence(vec![tx]);
    let r = conn
        .q_once(
            &c,
            r#"[:find ?uuid
                            :in ?tx
                            :where [?x :foo/uuid ?uuid ?tx]]"#,
            inputs,
        )
        .expect("results")
        .into();
    match r {
        QueryResults::Rel(ref v) => {
            assert_eq!(
                *v,
                vec![vec![TypedValue::Uuid(
                    Uuid::from_str("cf62d552-6569-4d1b-b667-04703041dfc4").expect("Valid UUID")
                ),]]
                .into()
            );
        }
        _ => panic!("Expected query to work."),
    }
}

#[test]
fn test_fulltext() {
    let mut c = new_connection("").expect("Couldn't open conn.");
    let mut conn = Conn::connect(&mut c).expect("Couldn't open DB.");

    conn.transact(
        &mut c,
        r#"[
        [:db/add "a" :db/ident :foo/term]
        [:db/add "a" :db/valueType :db.type/string]
        [:db/add "a" :db/fulltext false]
        [:db/add "a" :db/cardinality :db.cardinality/many]

        [:db/add "s" :db/ident :foo/fts]
        [:db/add "s" :db/valueType :db.type/string]
        [:db/add "s" :db/fulltext true]
        [:db/add "s" :db/index true]
        [:db/add "s" :db/cardinality :db.cardinality/many]
    ]"#,
    )
    .unwrap();

    let v = conn
        .transact(
            &mut c,
            r#"[
        [:db/add "v" :foo/fts "hello darkness my old friend"]
        [:db/add "v" :foo/fts "I've come to talk with you again"]
    ]"#,
        )
        .unwrap()
        .tempids
        .get("v")
        .cloned()
        .expect("v was mapped");

    let r = conn
        .q_once(
            &c,
            r#"[:find [?x ?val ?score]
                            :where [(fulltext $ :foo/fts "darkness") [[?x ?val _ ?score]]]]"#,
            None,
        )
        .expect("results")
        .into();
    match r {
        QueryResults::Tuple(Some(vals)) => {
            let mut vals = vals.into_iter();
            match (vals.next(), vals.next(), vals.next(), vals.next()) {
                (
                    Some(Binding::Scalar(TypedValue::Ref(x))),
                    Some(Binding::Scalar(TypedValue::String(text))),
                    Some(Binding::Scalar(TypedValue::Double(score))),
                    None,
                ) => {
                    assert_eq!(x, v);
                    assert_eq!(text.as_str(), "hello darkness my old friend");
                    assert_approx_eq!(score, OrderedFloat(0.0f64));
                }
                _ => panic!("Unexpected results."),
            }
        }
        r => panic!("Unexpected results {:?}.", r),
    }

    let a = conn
        .transact(&mut c, r#"[[:db/add "a" :foo/term "talk"]]"#)
        .unwrap()
        .tempids
        .get("a")
        .cloned()
        .expect("a was mapped");

    // If you use a non-constant search term, it must be bound earlier in the query.
    let query = r#"[:find ?x ?val
                    :where
                    [(fulltext $ :foo/fts ?term) [[?x ?val]]]
                    [?a :foo/term ?term]
                    ]"#;
    let r = conn.q_once(&c, query, None);
    match r.expect_err("expected query to fail") {
        MentatError::AlgebrizerError(
            query_algebrizer_traits::errors::AlgebrizerError::InvalidArgument(
                PlainSymbol(s),
                ty,
                i,
            ),
        ) => {
            assert_eq!(s, "fulltext");
            assert_eq!(ty, "string");
            assert_eq!(i, 2);
        }
        _ => panic!("Expected query to fail."),
    }

    // Bound to the wrong type? Error.
    let query = r#"[:find ?x ?val
                    :where
                    [?a :foo/term ?term]
                    [(fulltext $ :foo/fts ?a) [[?x ?val]]]]"#;
    let r = conn.q_once(&c, query, None);
    match r.expect_err("expected query to fail") {
        MentatError::AlgebrizerError(
            query_algebrizer_traits::errors::AlgebrizerError::InvalidArgument(
                PlainSymbol(s),
                ty,
                i,
            ),
        ) => {
            assert_eq!(s, "fulltext");
            assert_eq!(ty, "string");
            assert_eq!(i, 2);
        }
        _ => panic!("Expected query to fail."),
    }

    // If it's bound, and the right type, it'll work!
    let query = r#"[:find ?x ?val
                    :in ?a
                    :where
                    [?a :foo/term ?term]
                    [(fulltext $ :foo/fts ?term) [[?x ?val]]]]"#;
    let inputs = QueryInputs::with_value_sequence(vec![(
        Variable::from_valid_name("?a"),
        TypedValue::Ref(a),
    )]);
    let r = conn.q_once(&c, query, inputs).expect("results").into();
    match r {
        QueryResults::Rel(rels) => {
            let values: Vec<Vec<Binding>> = rels.into_iter().collect();
            assert_eq!(
                values,
                vec![vec![
                    Binding::Scalar(TypedValue::Ref(v)),
                    "I've come to talk with you again".into(),
                ]]
            );
        }
        _ => panic!("Expected query to work."),
    }
}

#[test]
fn test_instant_range_query() {
    let mut c = new_connection("").expect("Couldn't open conn.");
    let mut conn = Conn::connect(&mut c).expect("Couldn't open DB.");

    conn.transact(
        &mut c,
        r#"[
        [:db/add "a" :db/ident :foo/date]
        [:db/add "a" :db/valueType :db.type/instant]
        [:db/add "a" :db/cardinality :db.cardinality/one]
    ]"#,
    )
    .unwrap();

    let ids = conn
        .transact(
            &mut c,
            r#"[
        [:db/add "b" :foo/date #inst "2016-01-01T11:00:00.000Z"]
        [:db/add "c" :foo/date #inst "2016-06-01T11:00:01.000Z"]
        [:db/add "d" :foo/date #inst "2017-01-01T11:00:02.000Z"]
        [:db/add "e" :foo/date #inst "2017-06-01T11:00:03.000Z"]
    ]"#,
        )
        .unwrap()
        .tempids;

    let r = conn
        .q_once(
            &c,
            r#"[:find [?x ...]
                            :order (asc ?date)
                            :where
                            [?x :foo/date ?date]
                            [(< ?date #inst "2017-01-01T11:00:02.000Z")]]"#,
            None,
        )
        .expect("results")
        .into();
    match r {
        QueryResults::Coll(vals) => {
            assert_eq!(
                vals,
                vec![
                    Binding::Scalar(TypedValue::Ref(*ids.get("b").unwrap())),
                    Binding::Scalar(TypedValue::Ref(*ids.get("c").unwrap()))
                ]
            );
        }
        _ => panic!("Expected query to work."),
    }
}

#[test]
fn test_lookup() {
    let mut c = new_connection("").expect("Couldn't open conn.");
    let mut conn = Conn::connect(&mut c).expect("Couldn't open DB.");

    conn.transact(
        &mut c,
        r#"[
        [:db/add "a" :db/ident :foo/date]
        [:db/add "a" :db/valueType :db.type/instant]
        [:db/add "a" :db/cardinality :db.cardinality/one]
        [:db/add "b" :db/ident :foo/many]
        [:db/add "b" :db/valueType :db.type/long]
        [:db/add "b" :db/cardinality :db.cardinality/many]
    ]"#,
    )
    .unwrap();

    let ids = conn
        .transact(
            &mut c,
            r#"[
        [:db/add "b" :foo/many 123]
        [:db/add "b" :foo/many 456]
        [:db/add "b" :foo/date #inst "2016-01-01T11:00:00.000Z"]
        [:db/add "c" :foo/date #inst "2016-06-01T11:00:01.000Z"]
        [:db/add "d" :foo/date #inst "2017-01-01T11:00:02.000Z"]
        [:db/add "e" :foo/date #inst "2017-06-01T11:00:03.000Z"]
    ]"#,
        )
        .unwrap()
        .tempids;

    let entid = ids.get("b").unwrap();
    let foo_date = kw!(:foo/date);
    let foo_many = kw!(:foo/many);
    let db_ident = kw!(:db/ident);
    let expected =
        TypedValue::Instant(DateTime::<Utc>::from_str("2016-01-01T11:00:00.000Z").unwrap());

    // Fetch a value.
    assert_eq!(
        expected,
        conn.lookup_value_for_attribute(&c, *entid, &foo_date)
            .unwrap()
            .unwrap()
    );

    // Try to fetch a missing attribute.
    assert!(conn
        .lookup_value_for_attribute(&c, *entid, &db_ident)
        .unwrap()
        .is_none());

    // Try to fetch from a non-existent entity.
    assert!(conn
        .lookup_value_for_attribute(&c, 12344567, &foo_date)
        .unwrap()
        .is_none());

    // Fetch a multi-valued property.
    let two_longs = vec![TypedValue::Long(123), TypedValue::Long(456)];
    let fetched_many = conn
        .lookup_value_for_attribute(&c, *entid, &foo_many)
        .unwrap()
        .unwrap();
    assert!(two_longs.contains(&fetched_many));
}

#[test]
fn test_aggregates_type_handling() {
    let mut store = Store::open("").expect("opened");
    store
        .transact(
            r#"[
        {:db/ident :test/boolean :db/valueType :db.type/boolean :db/cardinality :db.cardinality/one}
        {:db/ident :test/long    :db/valueType :db.type/long    :db/cardinality :db.cardinality/one}
        {:db/ident :test/double  :db/valueType :db.type/double  :db/cardinality :db.cardinality/one}
        {:db/ident :test/string  :db/valueType :db.type/string  :db/cardinality :db.cardinality/one}
        {:db/ident :test/keyword :db/valueType :db.type/keyword :db/cardinality :db.cardinality/one}
        {:db/ident :test/uuid    :db/valueType :db.type/uuid    :db/cardinality :db.cardinality/one}
        {:db/ident :test/instant :db/valueType :db.type/instant :db/cardinality :db.cardinality/one}
        {:db/ident :test/ref     :db/valueType :db.type/ref     :db/cardinality :db.cardinality/one}
    ]"#,
        )
        .unwrap();

    store
        .transact(
            r#"[
        {:test/boolean false
         :test/long    10
         :test/double  2.4
         :test/string  "one"
         :test/keyword :foo/bar
         :test/uuid    #uuid "55555234-1234-1234-1234-123412341234"
         :test/instant #inst "2017-01-01T11:00:00.000Z"
         :test/ref     1}
        {:test/boolean true
         :test/long    20
         :test/double  4.4
         :test/string  "two"
         :test/keyword :foo/baz
         :test/uuid    #uuid "66666234-1234-1234-1234-123412341234"
         :test/instant #inst "2018-01-01T11:00:00.000Z"
         :test/ref     2}
        {:test/boolean true
         :test/long    30
         :test/double  6.4
         :test/string  "three"
         :test/keyword :foo/noo
         :test/uuid    #uuid "77777234-1234-1234-1234-123412341234"
         :test/instant #inst "2019-01-01T11:00:00.000Z"
         :test/ref     3}
    ]"#,
        )
        .unwrap();

    // No type limits => can't do it.
    let r = store.q_once(r#"[:find (sum ?v) . :where [_ _ ?v]]"#, None);
    let all_types = ValueTypeSet::any();
    match r.expect_err("expected query to fail") {
        MentatError::ProjectorError(
            ::query_projector_traits::errors::ProjectorError::CannotApplyAggregateOperationToTypes(
                SimpleAggregationOp::Sum,
                types,
            ),
        ) => {
            assert_eq!(types, all_types);
        }
        e => panic!("Unexpected error type {:?}", e),
    }

    // You can't sum instants.
    let r = store.q_once(
        r#"[:find (sum ?v) .
                             :where [_ _ ?v] [(type ?v :db.type/instant)]]"#,
        None,
    );
    match r.expect_err("expected query to fail") {
        MentatError::ProjectorError(
            ::query_projector_traits::errors::ProjectorError::CannotApplyAggregateOperationToTypes(
                SimpleAggregationOp::Sum,
                types,
            ),
        ) => {
            assert_eq!(types, ValueTypeSet::of_one(ValueType::Instant));
        }
        e => panic!("Unexpected error type {:?}", e),
    }

    // But you can count them.
    let r = store
        .q_once(
            r#"[:find (count ?v) .
                             :where [_ _ ?v] [(type ?v :db.type/instant)]]"#,
            None,
        )
        .into_scalar_result()
        .expect("results")
        .unwrap();

    // Our two transactions, the bootstrap transaction, plus the three values.
    assert_eq!(Binding::Scalar(TypedValue::Long(6)), r);

    // And you can min them, which returns an instant.
    let r = store
        .q_once(
            r#"[:find (min ?v) .
                             :where [_ _ ?v] [(type ?v :db.type/instant)]]"#,
            None,
        )
        .into_scalar_result()
        .expect("results")
        .unwrap();

    let earliest = DateTime::parse_from_rfc3339("2017-01-01T11:00:00.000Z")
        .unwrap()
        .with_timezone(&Utc);
    assert_eq!(Binding::Scalar(TypedValue::Instant(earliest)), r);

    let r = store
        .q_once(
            r#"[:find (sum ?v) .
                             :where [_ _ ?v] [(type ?v :db.type/long)]]"#,
            None,
        )
        .into_scalar_result()
        .expect("results")
        .unwrap();

    // Yes, the current version is in the store as a Long!
    let total = 30i64 + 20i64 + 10i64 + ::mentat_db::db::CURRENT_VERSION as i64;
    assert_eq!(Binding::Scalar(TypedValue::Long(total)), r);

    let r = store
        .q_once(
            r#"[:find (avg ?v) .
                             :where [_ _ ?v] [(type ?v :db.type/double)]]"#,
            None,
        )
        .into_scalar_result()
        .expect("results")
        .unwrap();

    let avg = (6.4f64 / 3f64) + (4.4f64 / 3f64) + (2.4f64 / 3f64);
    assert_eq!(Binding::Scalar(TypedValue::Double(avg.into())), r);
}

#[test]
fn test_type_reqs() {
    let mut c = new_connection("").expect("Couldn't open conn.");
    let mut conn = Conn::connect(&mut c).expect("Couldn't open DB.");

    conn.transact(
        &mut c,
        r#"[
        {:db/ident :test/boolean :db/valueType :db.type/boolean :db/cardinality :db.cardinality/one}
        {:db/ident :test/long    :db/valueType :db.type/long    :db/cardinality :db.cardinality/one}
        {:db/ident :test/double  :db/valueType :db.type/double  :db/cardinality :db.cardinality/one}
        {:db/ident :test/string  :db/valueType :db.type/string  :db/cardinality :db.cardinality/one}
        {:db/ident :test/keyword :db/valueType :db.type/keyword :db/cardinality :db.cardinality/one}
        {:db/ident :test/uuid    :db/valueType :db.type/uuid    :db/cardinality :db.cardinality/one}
        {:db/ident :test/instant :db/valueType :db.type/instant :db/cardinality :db.cardinality/one}
        {:db/ident :test/ref     :db/valueType :db.type/ref     :db/cardinality :db.cardinality/one}
        {:db/ident :test/bytes   :db/valueType :db.type/bytes   :db/cardinality :db.cardinality/one}
    ]"#,
    )
    .unwrap();

    conn.transact(
        &mut c,
        r#"[
        {:test/boolean true
         :test/long    33
         :test/double  1.4
         :test/string  "foo"
         :test/keyword :foo/bar
         :test/uuid    #uuid "12341234-1234-1234-1234-123412341234"
         :test/instant #inst "2018-01-01T11:00:00.000Z"
         :test/ref     1
         :test/bytes   #bytes 010203050403022a }
    ]"#,
    )
    .unwrap();

    let eid_query = r#"[:find ?eid :where [?eid :test/string "foo"]]"#;

    let res = conn
        .q_once(&c, eid_query, None)
        .into_rel_result()
        .expect("results");

    assert_eq!(res.row_count(), 1);
    assert_eq!(res.width, 1);
    let entid = match res.into_iter().next().unwrap().into_iter().next().unwrap() {
        Binding::Scalar(TypedValue::Ref(eid)) => eid,
        unexpected => {
            panic!(
                "Query to get the entity id returned unexpected result {:?}",
                unexpected
            );
        }
    };

    for value_type in ValueType::all_enums().iter() {
        let q = format!(
            "[:find [?v ...] :in ?e :where [?e _ ?v] [(type ?v {})]]",
            value_type.into_keyword()
        );
        let results = conn
            .q_once(
                &c,
                &q,
                QueryInputs::with_value_sequence(vec![(
                    Variable::from_valid_name("?e"),
                    TypedValue::Ref(entid),
                )]),
            )
            .expect("results")
            .into();
        match results {
            QueryResults::Coll(vals) => {
                assert_eq!(vals.len(), 1, "Query should find exactly 1 item");
            }
            v => {
                panic!("Query returned unexpected type: {:?}", v);
            }
        }
    }

    conn.transact(
        &mut c,
        r#"[
        {:db/ident :test/long2 :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
    ]"#,
    )
    .unwrap();

    conn.transact(&mut c, format!("[[:db/add {} :test/long2 5]]", entid))
        .unwrap();
    let longs_query = r#"[:find [?v ...]
                          :order (asc ?v)
                          :in ?e
                          :where [?e _ ?v] [(type ?v :db.type/long)]]"#;

    let res = conn
        .q_once(
            &c,
            longs_query,
            QueryInputs::with_value_sequence(vec![(
                Variable::from_valid_name("?e"),
                TypedValue::Ref(entid),
            )]),
        )
        .expect("results")
        .into();
    match res {
        QueryResults::Coll(vals) => assert_eq!(
            vals,
            vec![
                Binding::Scalar(TypedValue::Long(5)),
                Binding::Scalar(TypedValue::Long(33))
            ]
        ),
        v => {
            panic!("Query returned unexpected type: {:?}", v);
        }
    };
}

#[test]
fn test_monster_head_aggregates() {
    let mut store = Store::open("").expect("opened");
    let mut in_progress = store.begin_transaction().expect("began");

    in_progress
        .transact(
            r#"[
        {:db/ident       :monster/heads
         :db/valueType   :db.type/long
         :db/cardinality :db.cardinality/one}
        {:db/ident       :monster/name
         :db/valueType   :db.type/string
         :db/cardinality :db.cardinality/one
         :db/index       true
         :db/unique      :db.unique/identity}
        {:db/ident       :monster/weapon
         :db/valueType   :db.type/string
         :db/cardinality :db.cardinality/many}
    ]"#,
        )
        .expect("transacted");

    in_progress
        .transact(
            r#"[
        {:monster/heads  1
         :monster/name   "Medusa"
         :monster/weapon "Stony gaze"}
        {:monster/heads  1
         :monster/name   "Cyclops"
         :monster/weapon ["Large club" "Mighty arms" "Stompy feet"]}
        {:monster/heads  1
         :monster/name   "Chimera"
         :monster/weapon "Goat-like agility"}
        {:monster/heads  3
         :monster/name   "Cerberus"
         :monster/weapon ["8-foot Kong®" "Deadly drool"]}
    ]"#,
        )
        .expect("transacted");

    // Without :with, uniqueness applies prior to aggregation, so we get 1 + 3 = 4.
    let res = in_progress
        .q_once(
            "[:find (sum ?heads) . :where [?monster :monster/heads ?heads]]",
            None,
        )
        .expect("results")
        .into();
    match res {
        QueryResults::Scalar(Some(Binding::Scalar(TypedValue::Long(count)))) => {
            assert_eq!(count, 4);
        }
        r => panic!("Unexpected result {:?}", r),
    };

    // With :with, uniqueness includes the monster, so we get 1 + 1 + 1 + 3 = 6.
    let res = in_progress
        .q_once(
            "[:find (sum ?heads) . :with ?monster :where [?monster :monster/heads ?heads]]",
            None,
        )
        .expect("results")
        .into();
    match res {
        QueryResults::Scalar(Some(Binding::Scalar(TypedValue::Long(count)))) => {
            assert_eq!(count, 6);
        }
        r => panic!("Unexpected result {:?}", r),
    };

    // Aggregates group.
    let res = in_progress
        .q_once(
            r#"[:find ?name (count ?weapon)
                                     :with ?monster
                                     :order (asc ?name)
                                     :where [?monster :monster/name ?name]
                                            [?monster :monster/weapon ?weapon]]"#,
            None,
        )
        .expect("results")
        .into();
    match res {
        QueryResults::Rel(vals) => {
            let expected = vec![
                vec!["Cerberus".into(), TypedValue::Long(2)],
                vec!["Chimera".into(), TypedValue::Long(1)],
                vec!["Cyclops".into(), TypedValue::Long(3)],
                vec!["Medusa".into(), TypedValue::Long(1)],
            ];
            assert_eq!(vals, expected.into());
        }
        r => panic!("Unexpected result {:?}", r),
    };

    in_progress.rollback().expect("rolled back");
}

#[test]
fn test_basic_aggregates() {
    let mut store = Store::open("").expect("opened");

    store.transact(r#"[
        {:db/ident :foo/is-vegetarian :db/valueType :db.type/boolean :db/cardinality :db.cardinality/one}
        {:db/ident :foo/age           :db/valueType :db.type/long    :db/cardinality :db.cardinality/one}
        {:db/ident :foo/name          :db/valueType :db.type/string  :db/cardinality :db.cardinality/one}
    ]"#).unwrap();

    let _ids = store
        .transact(
            r#"[
        [:db/add "a" :foo/name "Alice"]
        [:db/add "b" :foo/name "Beli"]
        [:db/add "c" :foo/name "Carlos"]
        [:db/add "d" :foo/name "Diana"]
        [:db/add "a" :foo/is-vegetarian true]
        [:db/add "b" :foo/is-vegetarian true]
        [:db/add "c" :foo/is-vegetarian false]
        [:db/add "d" :foo/is-vegetarian false]
        [:db/add "a" :foo/age 14]
        [:db/add "b" :foo/age 22]
        [:db/add "c" :foo/age 42]
        [:db/add "d" :foo/age 28]
    ]"#,
        )
        .unwrap()
        .tempids;

    // Count the number of distinct bindings of `?veg` that are `true` -- namely, one.
    // This is not the same as `count-distinct`: note the distinction between
    // including `:with` and not.
    // In this case, the `DISTINCT` must occur inside the aggregation, not outside it.
    /*
    Rather than:

    SELECT DISTINCT count(1) AS `(count ?veg)`
    FROM `datoms` AS `datoms00`
    WHERE `datoms00`.a = 65536
      AND `datoms00`.v = 1;

    our query should be

    SELECT DISTINCT count(`?veg`) AS `(count ?veg)`
    FROM (
        SELECT DISTINCT 1 AS `?veg`
         FROM `datoms` AS `datoms00`
        WHERE `datoms00`.a = 65536
          AND `datoms00`.v = 1
    );
    */
    let r = store
        .q_once(
            r#"[:find (count ?veg)
                             :where
                             [_ :foo/is-vegetarian ?veg]
                             [(ground true) ?veg]]"#,
            None,
        )
        .expect("results")
        .into();
    match r {
        QueryResults::Rel(vals) => {
            assert_eq!(vals, vec![vec![TypedValue::Long(1)]].into());
        }
        _ => panic!("Expected rel."),
    }

    // And this should be
    /*
    SELECT DISTINCT count(`?veg`) AS `(count ?veg)`
    FROM (
        SELECT DISTINCT 1 AS `?veg`, `datoms00`.e AS `?person`
         FROM `datoms` AS `datoms00`
        WHERE `datoms00`.a = 65536
          AND `datoms00`.v = 1
    );
    */
    let r = store
        .q_once(
            r#"[:find (count ?veg) .
                             :with ?person
                             :where
                             [?person :foo/is-vegetarian ?veg]
                             [(ground true) ?veg]]"#,
            None,
        )
        .expect("results")
        .into();
    match r {
        QueryResults::Scalar(Some(val)) => {
            assert_eq!(val, Binding::Scalar(TypedValue::Long(2)));
        }
        _ => panic!("Expected scalar."),
    }

    // What are the oldest and youngest ages?
    let r = store
        .q_once(
            r#"[:find [(min ?age) (max ?age)]
                             :where
                             [_ :foo/age ?age]]"#,
            None,
        )
        .expect("results")
        .into();
    match r {
        QueryResults::Tuple(Some(vals)) => {
            assert_eq!(
                vals,
                vec![
                    Binding::Scalar(TypedValue::Long(14)),
                    Binding::Scalar(TypedValue::Long(42))
                ]
            );
        }
        _ => panic!("Expected tuple."),
    }

    // Who's youngest, via order?
    let r = store
        .q_once(
            r#"[:find [?name ?age]
                             :order (asc ?age)
                             :where
                             [?x :foo/age ?age]
                             [?x :foo/name ?name]]"#,
            None,
        )
        .expect("results")
        .into();
    match r {
        QueryResults::Tuple(Some(vals)) => {
            assert_eq!(
                vals,
                vec!["Alice".into(), Binding::Scalar(TypedValue::Long(14))]
            );
        }
        r => panic!("Unexpected results {:?}", r),
    }

    // Who's oldest, via order?
    let r = store
        .q_once(
            r#"[:find [?name ?age]
                             :order (desc ?age)
                             :where
                             [?x :foo/age ?age]
                             [?x :foo/name ?name]]"#,
            None,
        )
        .expect("results")
        .into();
    match r {
        QueryResults::Tuple(Some(vals)) => {
            assert_eq!(
                vals,
                vec!["Carlos".into(), Binding::Scalar(TypedValue::Long(42))]
            );
        }
        _ => panic!("Expected tuple."),
    }

    // How many of each age do we have?
    // Add an extra person to make this interesting.
    store
        .transact(r#"[{:foo/name "Medusa", :foo/age 28}]"#)
        .expect("transacted");

    // If we omit the 'with', we'll get the wrong answer:
    let r = store
        .q_once(
            r#"[:find ?age (count ?age)
                             :order (asc ?age)
                             :where [_ :foo/age ?age]]"#,
            None,
        )
        .expect("results")
        .into();

    match r {
        QueryResults::Rel(vals) => {
            assert_eq!(
                vals,
                vec![
                    vec![TypedValue::Long(14), TypedValue::Long(1)],
                    vec![TypedValue::Long(22), TypedValue::Long(1)],
                    vec![TypedValue::Long(28), TypedValue::Long(1)],
                    vec![TypedValue::Long(42), TypedValue::Long(1)],
                ]
                .into()
            );
        }
        _ => panic!("Expected rel."),
    }

    // If we include it, we'll get the right one:
    let r = store
        .q_once(
            r#"[:find ?age (count ?age)
                             :with ?person
                             :order (asc ?age)
                             :where [?person :foo/age ?age]]"#,
            None,
        )
        .expect("results")
        .into();

    match r {
        QueryResults::Rel(vals) => {
            assert_eq!(
                vals,
                vec![
                    vec![TypedValue::Long(14), TypedValue::Long(1)],
                    vec![TypedValue::Long(22), TypedValue::Long(1)],
                    vec![TypedValue::Long(28), TypedValue::Long(2)],
                    vec![TypedValue::Long(42), TypedValue::Long(1)],
                ]
                .into()
            );
        }
        _ => panic!("Expected rel."),
    }
}

#[test]
fn test_combinatorial() {
    let mut store = Store::open("").expect("opened");

    store
        .transact(
            r#"[
        [:db/add "a" :db/ident :foo/name]
        [:db/add "a" :db/valueType :db.type/string]
        [:db/add "a" :db/cardinality :db.cardinality/one]
        [:db/add "b" :db/ident :foo/dance]
        [:db/add "b" :db/valueType :db.type/ref]
        [:db/add "b" :db/cardinality :db.cardinality/many]
        [:db/add "b" :db/index true]
    ]"#,
        )
        .unwrap();

    store
        .transact(
            r#"[
        [:db/add "a" :foo/name "Alice"]
        [:db/add "b" :foo/name "Beli"]
        [:db/add "c" :foo/name "Carlos"]
        [:db/add "d" :foo/name "Diana"]

        ;; Alice danced with Beli twice.
        [:db/add "a"  :foo/dance "ab"]
        [:db/add "b"  :foo/dance "ab"]
        [:db/add "a"  :foo/dance "ba"]
        [:db/add "b"  :foo/dance "ba"]

        ;; Carlos danced with Diana.
        [:db/add "c"  :foo/dance "cd"]
        [:db/add "d"  :foo/dance "cd"]

        ;; Alice danced with Diana.
        [:db/add "a"  :foo/dance "ad"]
        [:db/add "d"  :foo/dance "ad"]

   ]"#,
        )
        .unwrap();

    // How many different pairings of dancers were there?
    // If we just use `!=` (or `differ`), the number is doubled because of symmetry!
    assert_eq!(
        Binding::Scalar(TypedValue::Long(6)),
        store
            .q_once(
                r#"[:find (count ?right) .
                                :with ?left
                                :where
                                [?left :foo/dance ?dance]
                                [?right :foo/dance ?dance]
                                [(differ ?left ?right)]]"#,
                None
            )
            .into_scalar_result()
            .expect("scalar results")
            .unwrap()
    );

    // SQL addresses this by using `<` instead of `!=` -- by imposing
    // an order on values, we can ensure that each pair only appears once, not
    // once per permutation.
    // It's far from ideal to expose an ordering on entids, because developers
    // will come to rely on it. Instead we expose a specific operator: `unpermute`.
    // When used in a query that generates permuted pairs of references, this
    // ensures that only one permutation is returned for a given pair.
    assert_eq!(
        Binding::Scalar(TypedValue::Long(3)),
        store
            .q_once(
                r#"[:find (count ?right) .
                                :with ?left
                                :where
                                [?left :foo/dance ?dance]
                                [?right :foo/dance ?dance]
                                [(unpermute ?left ?right)]]"#,
                None
            )
            .into_scalar_result()
            .expect("scalar results")
            .unwrap()
    );
}

#[test]
fn test_aggregate_the() {
    let mut store = Store::open("").expect("opened");

    store
        .transact(
            r#"[
        {:db/ident       :visit/visitedOnDevice
         :db/valueType   :db.type/ref
         :db/cardinality :db.cardinality/one}
        {:db/ident       :visit/visitAt
         :db/valueType   :db.type/instant
         :db/cardinality :db.cardinality/one}
        {:db/ident       :site/visit
         :db/valueType   :db.type/ref
         :db/isComponent true
         :db/cardinality :db.cardinality/many}
        {:db/ident       :site/url
         :db/valueType   :db.type/string
         :db/unique      :db.unique/identity
         :db/cardinality :db.cardinality/one
         :db/index       true}
        {:db/ident       :visit/page
         :db/valueType   :db.type/ref
         :db/isComponent true                    ; Debatable.
         :db/cardinality :db.cardinality/one}
        {:db/ident       :page/title
         :db/valueType   :db.type/string
         :db/fulltext    true
         :db/index       true
         :db/cardinality :db.cardinality/one}
        {:db/ident       :visit/container
         :db/valueType   :db.type/ref
         :db/cardinality :db.cardinality/one}
    ]"#,
        )
        .expect("transacted schema");

    store
        .transact(
            r#"[
        {:db/ident :container/facebook}
        {:db/ident :container/personal}

        {:db/ident :device/my-desktop}
    ]"#,
        )
        .expect("transacted idents");

    store
        .transact(
            r#"[
        {:visit/visitedOnDevice :device/my-desktop
         :visit/visitAt #inst "2018-04-06T20:46:00Z"
         :visit/container :container/facebook
         :db/id "another"
         :visit/page "fbpage2"}
        {:db/id "fbpage2"
         :page/title "(1) Facebook"}
        {:visit/visitedOnDevice :device/my-desktop
         :visit/visitAt #inst "2018-04-06T18:46:00Z"
         :visit/container :container/facebook
         :db/id "fbvisit"
         :visit/page "fbpage"}
        {:db/id "fbpage"
         :page/title "(2) Facebook"}
        {:site/url "https://www.facebook.com"
         :db/id "aa"
         :site/visit ["personalvisit" "another" "fbvisit"]}
        {:visit/visitedOnDevice :device/my-desktop
         :visit/visitAt #inst "2018-04-06T18:46:00Z"
         :visit/container :container/personal
         :db/id "personalvisit"
         :visit/page "personalpage"}
        {:db/id "personalpage"
         :page/title "Facebook - Log In or Sign Up"}
    ]"#,
        )
        .expect("transacted data");

    let per_title = store
        .q_once(
            r#"
            [:find (max ?visitDate) ?title
             :where [?site :site/url "https://www.facebook.com"]
                    [?site :site/visit ?visit]
                    [?visit :visit/container :container/facebook]
                    [?visit :visit/visitAt ?visitDate]
                    [?visit :visit/page ?page]
                    [?page :page/title ?title]]"#,
            None,
        )
        .into_rel_result()
        .expect("two results");

    let corresponding_title = store
        .q_once(
            r#"
            [:find (max ?visitDate) (the ?title)
             :where [?site :site/url "https://www.facebook.com"]
                    [?site :site/visit ?visit]
                    [?visit :visit/container :container/facebook]
                    [?visit :visit/visitAt ?visitDate]
                    [?visit :visit/page ?page]
                    [?page :page/title ?title]]"#,
            None,
        )
        .into_rel_result()
        .expect("one result");

    // This test shows the distinction between `?title` and `(the ?title`) — the former returns two
    // results, while the latter returns one. Without `the` we group by `?title`, getting the
    // maximum visit date for each title; with it we don't group by value, instead getting the title
    // that corresponds to the maximum visit date.
    //
    // 'Group' in this context translates to GROUP BY in the generated SQL.
    assert_eq!(2, per_title.row_count());
    assert_eq!(1, corresponding_title.row_count());

    assert_eq!(
        corresponding_title,
        vec![vec![
            TypedValue::Instant(DateTime::<Utc>::from_str("2018-04-06T20:46:00.000Z").unwrap()),
            TypedValue::typed_string("(1) Facebook")
        ]]
        .into()
    );
}

#[test]
fn test_null_aggregates() {
    let store = Store::open("").expect("opened");

    let rel = store
        .q_once(
            r#"
            [:find (count ?tx) (max ?txInstant)
             :where [_ _ _ ?tx]
                    [?tx :db/txInstant ?txInstant]
                    [(< ?txInstant #inst "2016-01-01T11:00:00.000Z")]
            ]"#,
            None,
        )
        .into_rel_result()
        .expect("no results");

    // (count ?tx) is 0, but (max ?txInstant) is over 0 SQL rows, yielding a NULL in the SQL rows.
    // We reject the entire row containing NULL aggregates.
    assert_eq!(0, rel.row_count());

    let rel_pull = store
        .q_once(
            r#"
            [:find (count ?tx) (max ?txInstant) (pull ?tx [*])
             :where [_ _ _ ?tx]
                    [?tx :db/txInstant ?txInstant]
                    [(< ?txInstant #inst "2016-01-01T11:00:00.000Z")]
            ]"#,
            None,
        )
        .into_rel_result()
        .expect("no results");

    // Same logic as above -- just verifying that `RelTwoStagePullProjector` handles NULL.
    assert_eq!(0, rel_pull.row_count());

    let coll = store
        .q_once(
            r#"
            [:find [(max ?txInstant) ...]
             :where [_ _ _ ?tx]
                    [?tx :db/txInstant ?txInstant]
                    [(< ?txInstant #inst "2016-01-01T11:00:00.000Z")]
            ]"#,
            None,
        )
        .into_coll_result()
        .expect("no results");

    // (max ?txInstant) is over 0 SQL rows, yielding a NULL in the SQL rows.  We reject the entire
    // row containing NULL aggregates, yielding an empty vector of results.
    assert_eq!(coll, vec![]);

    let tuple = store
        .q_once(
            r#"
            [:find [(count ?tx) (max ?txInstant)]
             :where [_ _ _ ?tx]
                    [?tx :db/txInstant ?txInstant]
                    [(< ?txInstant #inst "2016-01-01T11:00:00.000Z")]
            ]"#,
            None,
        )
        .into_tuple_result()
        .expect("no results");

    // (count ?tx) is 0, but (max ?txInstant) is over 0 SQL rows, yielding a NULL in the SQL rows.
    // We reject the entire row containing NULL aggregates, yielding no tuple result at all.
    assert_eq!(tuple, None);

    let tuple_pull = store
        .q_once(
            r#"
            [:find [(count ?tx) (max ?txInstant) (pull ?tx [*])]
             :where [_ _ _ ?tx]
                    [?tx :db/txInstant ?txInstant]
                    [(< ?txInstant #inst "2016-01-01T11:00:00.000Z")]
            ]"#,
            None,
        )
        .into_tuple_result()
        .expect("no results");

    // Same logic as above -- just verifying that `CollTwoStagePullProjector` handles NULL.
    assert_eq!(tuple_pull, None);

    let scalar = store
        .q_once(
            r#"
            [:find (max ?txInstant) .
             :where [_ _ _ ?tx]
                    [?tx :db/txInstant ?txInstant]
                    [(< ?txInstant #inst "2016-01-01T11:00:00.000Z")]
            ]"#,
            None,
        )
        .into_scalar_result()
        .expect("no results");

    // (max ?txInstant) is over 0 SQL rows, yielding a NULL in the SQL rows.  We reject the entire
    // row containing NULL aggregates, yielding no scalar result at all.
    assert_eq!(scalar, None);
}

#[test]
fn test_aggregation_implicit_grouping() {
    let mut store = Store::open("").expect("opened");

    store
        .transact(
            r#"[
        [:db/add "a" :db/ident :foo/score]
        [:db/add "a" :db/valueType :db.type/long]
        [:db/add "a" :db/cardinality :db.cardinality/one]
        [:db/add "b" :db/ident :foo/name]
        [:db/add "b" :db/valueType :db.type/string]
        [:db/add "b" :db/cardinality :db.cardinality/one]
        [:db/add "c" :db/ident :foo/is-vegetarian]
        [:db/add "c" :db/valueType :db.type/boolean]
        [:db/add "c" :db/cardinality :db.cardinality/one]
        [:db/add "d" :db/ident :foo/play]
        [:db/add "d" :db/valueType :db.type/ref]
        [:db/add "d" :db/cardinality :db.cardinality/many]
        [:db/add "d" :db/index true]
        [:db/add "d" :db/unique :db.unique/value]
    ]"#,
        )
        .unwrap();

    let ids = store
        .transact(
            r#"[
        [:db/add "a" :foo/name "Alice"]
        [:db/add "b" :foo/name "Beli"]
        [:db/add "c" :foo/name "Carlos"]
        [:db/add "d" :foo/name "Diana"]
        [:db/add "a" :foo/is-vegetarian true]
        [:db/add "b" :foo/is-vegetarian true]
        [:db/add "c" :foo/is-vegetarian false]
        [:db/add "d" :foo/is-vegetarian false]
        [:db/add "aa" :foo/score 14]
        [:db/add "ab" :foo/score 99]
        [:db/add "ac" :foo/score 14]
        [:db/add "ba" :foo/score 22]
        [:db/add "bb" :foo/score 11]
        [:db/add "ca" :foo/score 42]
        [:db/add "da" :foo/score 5]
        [:db/add "db" :foo/score 28]
        [:db/add "d"  :foo/play "da"]
        [:db/add "d"  :foo/play "db"]
        [:db/add "a"  :foo/play "aa"]
        [:db/add "a"  :foo/play "ab"]
        [:db/add "a"  :foo/play "ac"]
        [:db/add "b"  :foo/play "ba"]
        [:db/add "b"  :foo/play "bb"]
        [:db/add "c"  :foo/play "ca"]
    ]"#,
        )
        .unwrap()
        .tempids;

    // How many different scores were there?
    assert_eq!(
        Binding::Scalar(TypedValue::Long(7)),
        store
            .q_once(
                r#"[:find (count ?score) .
                                :where
                                [?game :foo/score ?score]]"#,
                None
            )
            .into_scalar_result()
            .expect("scalar results")
            .unwrap()
    );

    // How many different games resulted in scores?
    // '14' appears twice.
    assert_eq!(
        Binding::Scalar(TypedValue::Long(8)),
        store
            .q_once(
                r#"[:find (count ?score) .
                                :with ?game
                                :where
                                [?game :foo/score ?score]]"#,
                None
            )
            .into_scalar_result()
            .expect("scalar results")
            .unwrap()
    );

    // Who's the highest-scoring vegetarian?
    assert_eq!(
        vec!["Alice".into(), Binding::Scalar(TypedValue::Long(99))],
        store
            .q_once(
                r#"[:find [(the ?name) (max ?score)]
                                :where
                                [?game :foo/score ?score]
                                [?person :foo/play ?game]
                                [?person :foo/is-vegetarian true]
                                [?person :foo/name ?name]]"#,
                None
            )
            .into_tuple_result()
            .expect("tuple results")
            .unwrap()
    );

    // We can't run an ambiguous correspondence.
    let res = store.q_once(
        r#"[:find [(the ?name) (min ?score) (max ?score)]
                               :where
                               [?game :foo/score ?score]
                               [?person :foo/play ?game]
                               [?person :foo/is-vegetarian true]
                               [?person :foo/name ?name]]"#,
        None,
    );
    match res.expect_err("expected query to fail") {
        MentatError::ProjectorError(
            ::query_projector_traits::errors::ProjectorError::AmbiguousAggregates(mmc, cc),
        ) => {
            assert_eq!(mmc, 2);
            assert_eq!(cc, 1);
        }
        e => {
            panic!("Unexpected error type {:?}.", e);
        }
    }

    // Max scores for vegetarians.
    let expected: RelResult<Binding> = vec![
        vec!["Alice".into(), TypedValue::Long(99)],
        vec!["Beli".into(), TypedValue::Long(22)],
    ]
    .into();
    assert_eq!(
        expected,
        store
            .q_once(
                r#"[:find ?name (max ?score)
                                :where
                                [?game :foo/score ?score]
                                [?person :foo/play ?game]
                                [?person :foo/is-vegetarian true]
                                [?person :foo/name ?name]]"#,
                None
            )
            .into_rel_result()
            .expect("rel results")
    );

    // We can combine these aggregates.
    let r = store
        .q_once(
            r#"[:find ?x ?name (max ?score) (count ?score) (avg ?score)
                             :with ?game           ; So we don't discard duplicate scores!
                             :where
                             [?x :foo/name ?name]
                             [?x :foo/play ?game]
                             [?game :foo/score ?score]]"#,
            None,
        )
        .expect("results")
        .into();
    match r {
        QueryResults::Rel(vals) => {
            assert_eq!(
                vals,
                vec![
                    vec![
                        TypedValue::Ref(ids.get("a").cloned().unwrap()),
                        "Alice".into(),
                        TypedValue::Long(99),
                        TypedValue::Long(3),
                        TypedValue::Double((127f64 / 3f64).into())
                    ],
                    vec![
                        TypedValue::Ref(ids.get("b").cloned().unwrap()),
                        "Beli".into(),
                        TypedValue::Long(22),
                        TypedValue::Long(2),
                        TypedValue::Double((33f64 / 2f64).into())
                    ],
                    vec![
                        TypedValue::Ref(ids.get("c").cloned().unwrap()),
                        "Carlos".into(),
                        TypedValue::Long(42),
                        TypedValue::Long(1),
                        TypedValue::Double(42f64.into())
                    ],
                    vec![
                        TypedValue::Ref(ids.get("d").cloned().unwrap()),
                        "Diana".into(),
                        TypedValue::Long(28),
                        TypedValue::Long(2),
                        TypedValue::Double((33f64 / 2f64).into())
                    ]
                ]
                .into()
            );
        }
        x => panic!("Got unexpected results {:?}", x),
    }
}

#[test]
fn test_tx_ids() {
    let mut store = Store::open("").expect("opened");

    store
        .transact(
            r#"[
        [:db/add "a" :db/ident :foo/term]
        [:db/add "a" :db/valueType :db.type/string]
        [:db/add "a" :db/fulltext false]
        [:db/add "a" :db/cardinality :db.cardinality/many]
    ]"#,
        )
        .unwrap();

    let tx1 = store
        .transact(
            r#"[
        [:db/add "v" :foo/term "1"]
    ]"#,
        )
        .expect("tx1 to apply")
        .tx_id;

    let tx2 = store
        .transact(
            r#"[
        [:db/add "v" :foo/term "2"]
    ]"#,
        )
        .expect("tx2 to apply")
        .tx_id;

    let tx3 = store
        .transact(
            r#"[
        [:db/add "v" :foo/term "3"]
    ]"#,
        )
        .expect("tx3 to apply")
        .tx_id;

    fn assert_tx_id_range(store: &Store, after: Entid, before: Entid, expected: Vec<TypedValue>) {
        // TODO: after https://github.com/mozilla/mentat/issues/641, use q_prepare with inputs bound
        // at execution time.
        let r = store
            .q_once(
                r#"[:find [?tx ...]
                                 :in ?after ?before
                                 :where
                                 [(tx-ids $ ?after ?before) [?tx ...]]
                                ]"#,
                QueryInputs::with_value_sequence(vec![
                    (Variable::from_valid_name("?after"), TypedValue::Ref(after)),
                    (
                        Variable::from_valid_name("?before"),
                        TypedValue::Ref(before),
                    ),
                ]),
            )
            .expect("results")
            .into();
        match r {
            QueryResults::Coll(txs) => {
                let expected: Vec<Binding> = expected.into_iter().map(|tv| tv.into()).collect();
                assert_eq!(txs, expected);
            }
            x => panic!("Got unexpected results {:?}", x),
        }
    }

    assert_tx_id_range(&store, tx1, tx2, vec![TypedValue::Ref(tx1)]);
    assert_tx_id_range(
        &store,
        tx1,
        tx3,
        vec![TypedValue::Ref(tx1), TypedValue::Ref(tx2)],
    );
    assert_tx_id_range(&store, tx2, tx3, vec![TypedValue::Ref(tx2)]);
    assert_tx_id_range(
        &store,
        tx2,
        tx3 + 1,
        vec![TypedValue::Ref(tx2), TypedValue::Ref(tx3)],
    );
}

fn run_tx_data_test(mut store: Store) {
    store
        .transact(
            r#"[
        [:db/add "a" :db/ident :foo/term]
        [:db/add "a" :db/valueType :db.type/string]
        [:db/add "a" :db/fulltext false]
        [:db/add "a" :db/cardinality :db.cardinality/many]
    ]"#,
        )
        .unwrap();

    let tx1 = store
        .transact(
            r#"[
        [:db/add "e" :foo/term "1"]
    ]"#,
        )
        .expect("tx1 to apply");

    let tx2 = store
        .transact(
            r#"[
        [:db/add "e" :foo/term "2"]
    ]"#,
        )
        .expect("tx2 to apply");

    fn assert_tx_data(store: &Store, tx: &TxReport, value: TypedValue) {
        // TODO: after https://github.com/mozilla/mentat/issues/641, use q_prepare with inputs bound
        // at execution time.
        let r = store
            .q_once(
                r#"[:find ?e ?a-name ?v ?tx ?added
                                 :in ?tx-in
                                 :where
                                 [(tx-data $ ?tx-in) [[?e ?a ?v ?tx ?added]]]
                                 [?a :db/ident ?a-name]
                                 :order ?e
                                ]"#,
                QueryInputs::with_value_sequence(vec![(
                    Variable::from_valid_name("?tx-in"),
                    TypedValue::Ref(tx.tx_id),
                )]),
            )
            .expect("results")
            .into();

        let e = tx.tempids.get("e").cloned().expect("tempid");

        match r {
            QueryResults::Rel(vals) => {
                assert_eq!(
                    vals,
                    vec![
                        vec![
                            TypedValue::Ref(e),
                            TypedValue::typed_ns_keyword("foo", "term"),
                            value,
                            TypedValue::Ref(tx.tx_id),
                            TypedValue::Boolean(true)
                        ],
                        vec![
                            TypedValue::Ref(tx.tx_id),
                            TypedValue::typed_ns_keyword("db", "txInstant"),
                            TypedValue::Instant(tx.tx_instant),
                            TypedValue::Ref(tx.tx_id),
                            TypedValue::Boolean(true)
                        ],
                    ]
                    .into()
                );
            }
            x => panic!("Got unexpected results {:?}", x),
        }
    }

    assert_tx_data(&store, &tx1, "1".into());
    assert_tx_data(&store, &tx2, "2".into());
}

#[test]
fn test_tx_data() {
    run_tx_data_test(Store::open("").expect("opened"));
}

#[cfg(feature = "sqlite")]
#[test]
fn test_encrypted() {
    // We expect this to blow up completely if something is wrong with the encryption,
    // so the specific test we use doesn't matter that much.
    run_tx_data_test(Store::open_with_key("", "secret").expect("opened"));
}
