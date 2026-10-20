# Engineering journal

One file per lessons-learned category. Append an entry whenever something breaks in an instructive way or an
experiment produces numbers. These entries feed the report's "Lessons Learned" chapter; each member writes the
lessons for their own categories from these entries. Newest entries at the bottom.

Entry template (the course's lesson template):

```markdown
## YYYY-MM-DD: short title

* **Context / symptom:** what we saw (error text, metric, wrong number)
* **What we tried:** hypotheses and experiments, in order
* **Numbers:** measurements before/after (latency, throughput, size, counts)
* **Fix / decision:** what we changed (link the commit or file)
* **Lesson:** the general rule, one or two sentences
```

Categories: [ingestion](ingestion.md), [spark-processing](spark-processing.md), [streaming](streaming.md), [storage](storage.md), [integration](integration.md), [performance](performance.md), [monitoring](monitoring.md), [scaling](scaling.md), [quality-testing](quality-testing.md), [security](security.md), [fault-tolerance](fault-tolerance.md).
