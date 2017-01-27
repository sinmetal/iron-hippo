package org.sinmetal.flow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.datastore.DatastoreIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Key.PathElement;
import com.google.datastore.v1.Value;

public class BigQueryToDatastore {

	private static final String PROJECT_ID = "cp300demo1";
	private static final Logger LOG = LoggerFactory.getLogger(BigQueryToDatastore.class);

	static class GroupKeywords extends PTransform<PCollection<TableRow>, PCollection<KV<Integer, Iterable<TableRow>>>> {
		@Override
		public PCollection<KV<Integer, Iterable<TableRow>>> apply(PCollection<TableRow> rows) {

			// row... => keyword_id:value...
			PCollection<KV<Integer, TableRow>> kvs = rows.apply(ParDo.of(new TransferKeyValueFn()));

			// keyword_id, value =. keyword_id:List<value>
			PCollection<KV<Integer, Iterable<TableRow>>> group = kvs.apply(GroupByKey.<Integer, TableRow> create());

			return group;
		}
	}

	static class TransferKeyValueFn extends DoFn<TableRow, KV<Integer, TableRow>> {
		@Override
		public void processElement(ProcessContext c) {
			TableRow row = c.element();
			c.output(KV.of(Integer.parseInt(row.get("keyword_id").toString()), row));
		}
	}

	static class OutputLog extends DoFn<KV<Integer, Iterable<TableRow>>, Void> {
		@Override
		public void processElement(ProcessContext c) {
			KV<Integer, Iterable<TableRow>> kv = c.element();
			String keyword = "";
			StringBuffer bf = new StringBuffer();
			for (TableRow row : kv.getValue()) {
				String utterance = row.get("utterance").toString();
				bf.append(row.get("utterance"));
				bf.append(row.get(","));
				keyword = utterance;
			}
			LOG.info(kv.getKey().toString() + ":" + keyword + ":" + bf.toString());
		}
	}

	static class CreateEntities extends PTransform<PCollection<KV<Integer, Iterable<TableRow>>>, PCollection<Entity>> {

		@Override
		public PCollection<Entity> apply(PCollection<KV<Integer, Iterable<TableRow>>> input) {

			PCollection<Entity> entities = input.apply(ParDo.of(new CreateEntityFn()));

			return entities;
		}

	}

	static class CreateEntityFn extends DoFn<KV<Integer, Iterable<TableRow>>, Entity> {
		public Entity makeEntity(KV<Integer, Iterable<TableRow>> content) {
			Key key = Key.newBuilder()
					.addPath(PathElement.newBuilder().setKind("UtteranceTest").setId(content.getKey())).build();

			int characterCount = 0;
			String keyword = "";
			List<Value> list = new ArrayList<>();
			for (TableRow row : content.getValue()) {
				String utterance = row.get("utterance").toString();
				if (utterance == null || utterance.length() < 1) {
					continue;
				}
				String word = row.get("keyword").toString();
				if (keyword.equals(row.get("keyword")) == false) {
					keyword = word;
				}
				int len = utterance.length();
				// 200000
				if (characterCount + len > 200000) {
					LOG.info("Truncated the text. Length = " + characterCount + "keyword_id = " + content.getKey()
							+ ", keyword = " + word);
					break;
				}
				characterCount += len;
				list.add(Value.newBuilder().setStringValue(utterance).build());
			}

			Entity.Builder entityBuilder = Entity.newBuilder();
			entityBuilder.setKey(key);

			Map<String, Value> propertyMap = new HashMap<String, Value>();
			propertyMap.put("KeywordID", Value.newBuilder().setIntegerValue(content.getKey()).build());
			propertyMap.put("Keyword", Value.newBuilder().setStringValue(keyword).build());
			ArrayValue array = ArrayValue.newBuilder().addAllValues(list).build();
			propertyMap.put("Candidates", Value.newBuilder().setArrayValue(array).build());

			entityBuilder.putAllProperties(propertyMap);

			return entityBuilder.build();
		}

		@Override
		public void processElement(ProcessContext c) {
			c.output(makeEntity(c.element()));
		}
	}

	public static void main(String[] args) {
		Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());

		PCollection<KV<Integer, Iterable<TableRow>>> keywordGroups = p
				.apply(BigQueryIO.Read.named("ReadUtterance").from("topgate-ai-dev:dialog.utterance_llte50"))
				.apply(new GroupKeywords());

		PCollection<Entity> entities = keywordGroups.apply(new CreateEntities());
		entities.apply(DatastoreIO.v1().write().withProjectId(PROJECT_ID));

		p.run();
	}
}
