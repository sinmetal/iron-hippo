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
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.ValueProvider;
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

		String kind = "";

		public void setKind(String kind){
			this.kind = kind;
		}

		@Override
		public PCollection<Entity> apply(PCollection<KV<Integer, Iterable<TableRow>>> input) {

			CreateEntityFn f = new CreateEntityFn();
			f.setKind(this.kind);
			
			PCollection<Entity> entities = input.apply(ParDo.of(f));

			return entities;
		}

	}

	static class CreateEntityFn extends DoFn<KV<Integer, Iterable<TableRow>>, Entity> {
		
		String kind = "";

		public void setKind(String kind) {
			this.kind = kind;
		}
		
		public Entity makeEntity(KV<Integer, Iterable<TableRow>> content) {

			Key key = Key.newBuilder()
					.addPath(PathElement.newBuilder().setKind(this.kind).setId(content.getKey())).build();

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
				if (list.size() > 1000) {
					LOG.info("Truncated the text." + "keyword_id = " + content.getKey() + ", keyword = " + word);
					break;
				}
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

	public interface BigQueryToDatastoreOptions extends PipelineOptions {

		@Description("Path of the bigquery table to read from")
		@Default.String("cpb101demo1:samples.table")
		ValueProvider<String> getInputTable();

		@Description("Output destination Datastore ProjectID")
		@Default.String("cpb101demo1")
		ValueProvider<String> getOutputProjectID();

		@Description("Output destination Datastore Kind")
		@Default.String("hogeKind")
		ValueProvider<String> getOutputKind();

		void setInputTable(ValueProvider<String> value);

		void setOutputProjectID(ValueProvider<String> value);

		void setOutputKind(ValueProvider<String> value);
	}

	public static void main(String[] args) {
		BigQueryToDatastoreOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(BigQueryToDatastoreOptions.class);

		String inputTable = options.getInputTable().get();
		String projectID = options.getOutputProjectID().get();
		String kind = options.getOutputKind().get();

		LOG.info("Input_Table : " + inputTable);
		LOG.info("ProjectID : " + projectID);
		LOG.info("Kind : " + kind);

		Pipeline p = Pipeline.create(options);

		PCollection<KV<Integer, Iterable<TableRow>>> keywordGroups = p
				.apply(BigQueryIO.Read.named("ReadUtterance").from(inputTable)).apply(new GroupKeywords());
		
		CreateEntities createEntities = new CreateEntities();
		createEntities.setKind(kind);
		
		PCollection<Entity> entities = keywordGroups.apply(createEntities);
		entities.apply(DatastoreIO.v1().write().withProjectId(projectID));

		p.run();
	}
}
