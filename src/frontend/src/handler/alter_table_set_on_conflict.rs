// Copyright 2026 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use anyhow::Context;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::ConflictBehavior;
use risingwave_sqlparser::ast::{ObjectName, OnConflict, Statement};

use super::alter_table_column::fetch_table_catalog_for_alter;
use super::create_source::SqlColumnStrategy;
use super::{HandlerArgs, RwPgResponse, get_replace_table_plan};
use crate::error::{ErrorCode, Result};

/// Translate the AST [`OnConflict`] enum to the internal [`ConflictBehavior`]
/// for a non-append-only table. (`ALTER TABLE ... SET ON CONFLICT` is rejected
/// on append-only tables, so the more nuanced resolution in
/// `EitherOnConflict::to_behavior` is not needed here.)
fn on_conflict_to_behavior(on_conflict: OnConflict) -> ConflictBehavior {
    match on_conflict {
        OnConflict::UpdateFull => ConflictBehavior::Overwrite,
        OnConflict::Nothing => ConflictBehavior::IgnoreConflict,
        OnConflict::UpdateIfNotNull => ConflictBehavior::DoUpdateIfNotNull,
    }
}

pub async fn handle_alter_table_set_on_conflict(
    handler_args: HandlerArgs,
    table_name: ObjectName,
    new_on_conflict: OnConflict,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let (original_catalog, _has_incoming_sinks) =
        fetch_table_catalog_for_alter(session.as_ref(), &table_name)?;

    // ----- Validation -----

    // Append-only tables have their conflict behavior forced by the table
    // type (NoCheck with a generated row-id PK; IgnoreConflict with a user PK).
    // Changing it via ALTER would silently violate those invariants.
    if original_catalog.append_only {
        return Err(ErrorCode::NotSupported(
            "cannot change ON CONFLICT behavior on append-only tables".to_owned(),
            "the conflict behavior of an append-only table is fixed by its definition".to_owned(),
        )
        .into());
    }

    // `DO NOTHING` together with version columns is rejected at planning time
    // (see optimizer/mod.rs). Bail out early with a clearer message.
    if matches!(new_on_conflict, OnConflict::Nothing)
        && !original_catalog.version_column_indices.is_empty()
    {
        return Err(ErrorCode::InvalidInputSyntax(
            "ON CONFLICT DO NOTHING is incompatible with version columns".to_owned(),
        )
        .into());
    }

    let new_behavior = on_conflict_to_behavior(new_on_conflict);

    // No-op fast path: avoid the heavy replace_table machinery if the user
    // requests the behavior the table already has. This is also semantically
    // important because `Overwrite` may have been auto-downgraded to `NoCheck`
    // at runtime when the table has no downstreams; in that case the catalog
    // value is still `Overwrite` and asking for `OVERWRITE` is a true no-op.
    if new_behavior == original_catalog.conflict_behavior {
        return Ok(PgResponse::empty_result(StatementType::ALTER_TABLE));
    }

    // ----- Rewrite the stored CREATE TABLE definition -----

    let mut definition = original_catalog
        .create_sql_ast_purified()
        .context("unable to parse the original table definition")?;
    let Statement::CreateTable {
        on_conflict: ast_on_conflict,
        ..
    } = &mut definition
    else {
        // `fetch_table_catalog_for_alter` only returns user tables, whose
        // stored definition is always `CREATE TABLE`.
        unreachable!(
            "table catalog definition must be CREATE TABLE, got: {:?}",
            definition
        );
    };
    *ast_on_conflict = Some(new_on_conflict);

    // ----- Replan and replace -----

    let (source, table, graph, job_type) = Box::pin(get_replace_table_plan(
        &session,
        table_name,
        definition,
        &original_catalog,
        SqlColumnStrategy::FollowUnchecked,
    ))
    .await?;

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .replace_table(
            source.map(|x| x.to_prost()),
            table.to_prost(),
            graph,
            job_type,
        )
        .await?;

    Ok(PgResponse::empty_result(StatementType::ALTER_TABLE))
}
