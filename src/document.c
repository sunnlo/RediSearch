#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>
#include <time.h>

#include "forward_index.h"
#include "geo_index.h"
#include "index.h"
#include "numeric_filter.h"
#include "numeric_index.h"
#include "query.h"
#include "query_node.h"
#include "redis_index.h"
#include "rmutil/strings.h"
#include "rmutil/util.h"
#include "spec.h"
#include "tokenize.h"
#include "trie/trie_type.h"
#include "util/logging.h"
#include "varint.h"
#include "search_request.h"
#include "rmalloc.h"

/* Add a parsed document to the index. If replace is set, we will add it be deleting an older
 * version of it first */
int Document_AddToIndexes(Document *doc, RedisSearchCtx *ctx, const char **errorString,
                          int options) {
  int isnew = 1;
  int replace = options & DOCUMENT_ADD_REPLACE;
  int nosave = options & DOCUMENT_ADD_NOSAVE;

  // if we're in replace mode, first we need to try and delete the older version of the document
  if (replace) {
    DocTable_Delete(&ctx->spec->docs, RedisModule_StringPtrLen(doc->docKey, NULL));
  }

  doc->docId = DocTable_Put(&ctx->spec->docs, RedisModule_StringPtrLen(doc->docKey, NULL),
                            doc->score, 0, doc->payload, doc->payloadSize);

  // Make sure the document is not already in the index - it needs to be
  // incremental!
  if (doc->docId == 0 || !isnew) {
    *errorString = "Document already in index";
    return REDISMODULE_ERR;
  }

  // first save the document as hash
  if (nosave == 0 && Redis_SaveDocument(ctx, doc) != REDISMODULE_OK) {
    *errorString = "Could not save document data";
    return REDISMODULE_ERR;
  }

  ForwardIndex *idx = NewForwardIndex(doc);
  RSSortingVector *sv = NULL;
  if (ctx->spec->sortables) {
    sv = NewSortingVector(ctx->spec->sortables->len);
  }

  int totalTokens = 0;

  for (int i = 0; i < doc->numFields; i++) {
    // printf("Tokenizing %s: %s\n",
    // RedisModule_StringPtrLen(doc.fields[i].name, NULL),
    //        RedisModule_StringPtrLen(doc.fields[i].text, NULL));

    size_t len;
    const char *f = doc->fields[i].name;
    len = strlen(f);
    const char *c = RedisModule_StringPtrLen(doc->fields[i].text, NULL);

    FieldSpec *fs = IndexSpec_GetField(ctx->spec, f, len);
    if (fs == NULL) {
      LG_DEBUG("Skipping field %s not in index!", c);
      continue;
    }

    switch (fs->type) {
      case F_FULLTEXT:
        if (sv && fs->sortable) {
          RSSortingVector_Put(sv, fs->sortIdx, (void *)c, RS_SORTABLE_STR);
        }

        totalTokens = tokenize(c, fs->weight, fs->id, idx, forwardIndexTokenFunc, idx->stemmer,
                               totalTokens, ctx->spec->stopwords);
        break;
      case F_NUMERIC: {
        double score;

        if (RedisModule_StringToDouble(doc->fields[i].text, &score) == REDISMODULE_ERR) {
          *errorString = "Could not parse numeric index value";
          goto error;
        }

        NumericRangeTree *rt = OpenNumericIndex(ctx, fs->name);
        NumericRangeTree_Add(rt, doc->docId, score);

        // If this is a sortable numeric value - copy the value to the sorting vector
        if (sv && fs->sortable) {
          RSSortingVector_Put(sv, fs->sortIdx, &score, RS_SORTABLE_NUM);
        }
        break;
      }
      case F_GEO: {

        char *pos = strpbrk(c, " ,");
        if (!pos) {
          *errorString = "Invalid lon/lat format. Use \"lon lat\" or \"lon,lat\"";
          goto error;
        }
        *pos = '\0';
        pos++;
        char *slon = (char *)c, *slat = (char *)pos;

        GeoIndex gi = {.ctx = ctx, .sp = fs};
        if (GeoIndex_AddStrings(&gi, doc->docId, slon, slat) == REDISMODULE_ERR) {
          *errorString = "Could not index geo value";
          goto error;
        }
      }

      break;

      default:
        break;
    }
  }

  RSDocumentMetadata *md = DocTable_Get(&ctx->spec->docs, doc->docId);
  md->maxFreq = idx->maxFreq;
  if (sv) {
    DocTable_SetSortingVector(&ctx->spec->docs, doc->docId, sv);
  }

  // printf("totaltokens :%d\n", totalTokens);
  if (totalTokens > 0) {
    ForwardIndexIterator it = ForwardIndex_Iterate(idx);

    ForwardIndexEntry *entry = ForwardIndexIterator_Next(&it);

    while (entry != NULL) {
      // ForwardIndex_NormalizeFreq(idx, entry);
      int isNew = IndexSpec_AddTerm(ctx->spec, entry->term, entry->len);
      InvertedIndex *invidx = Redis_OpenInvertedIndex(ctx, entry->term, entry->len, 1);
      if (isNew) {
        ctx->spec->stats.numTerms += 1;
        ctx->spec->stats.termsSize += entry->len;
      }
      size_t sz = InvertedIndex_WriteEntry(invidx, entry);

      /*******************************************
      * update stats for the index
      ********************************************/

      /* record the change in capacity of the buffer */
      // ctx->spec->stats.invertedCap += w->bw.buf->cap - cap;
      // ctx->spec->stats.skipIndexesSize += w->skipIndexWriter.buf->cap - skcap;
      // ctx->spec->stats.scoreIndexesSize += w->scoreWriter.bw.buf->cap - sccap;
      /* record the actual size consumption change */
      ctx->spec->stats.invertedSize += sz;

      ctx->spec->stats.numRecords++;
      /* increment the number of terms if this is a new term*/

      /* Record the space saved for offset vectors */
      if (ctx->spec->flags & Index_StoreTermOffsets) {
        ctx->spec->stats.offsetVecsSize += entry->vw->bw.buf->offset;
        ctx->spec->stats.offsetVecRecords += entry->vw->nmemb;
      }
      // Redis_CloseWriter(w);

      entry = ForwardIndexIterator_Next(&it);
    }
    // ctx->spec->stats->numDocuments += 1;
  }
  ctx->spec->stats.numDocuments += 1;
  ForwardIndexFree(idx);
  return REDISMODULE_OK;

error:
  ForwardIndexFree(idx);

  return REDISMODULE_ERR;
}
