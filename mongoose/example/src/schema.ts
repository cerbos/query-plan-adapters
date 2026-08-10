/**
 * The demo domain's single resource kind, as a consumer would model it: one flat document with
 * scalar paths and no sub-documents. The richer schemas the adapter is PROVED against live in
 * ../../src/.
 *
 * `_id` carries the demo domain's own id (`"d1"`, `"d2"`, …) rather than an ObjectId. That is
 * ordinary Mongoose — declaring `_id` in the schema definition turns off the generated ObjectId —
 * and it keeps the id the example reports the id the store actually keys on, with nothing to
 * project between the two.
 */
import { Schema, model } from "mongoose";

export interface DocumentRow {
  _id: string;
  ownerId: string;
  /**
   * The policy calls this attribute `public`. The document path is `isPublic`, which is what
   * makes the mapper in `main.ts` earn its place: a Cerbos attribute name is not a document path,
   * and something has to say which path it means.
   */
  isPublic: boolean;
  // Never referenced by policy. `region` and `archived` are the application's own paths, and
  // ANDing them with the adapter's filter is usage shape 5.
  region: string;
  archived: boolean;
}

export const documentSchema = new Schema<DocumentRow>(
  {
    _id: { type: String, required: true },
    ownerId: { type: String, required: true },
    isPublic: { type: Boolean, required: true },
    region: { type: String, required: true },
    archived: { type: Boolean, required: true },
  },
  { versionKey: false }
);

export const DocumentModel = model<DocumentRow>("Document", documentSchema);
