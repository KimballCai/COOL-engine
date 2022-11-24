package com.nus.cool.core.cohort.refactor;

import com.nus.cool.core.cohort.refactor.birthselect.BirthSelection;
import com.nus.cool.core.cohort.refactor.filter.Filter;
import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import com.nus.cool.core.cohort.refactor.utils.DateUtils;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.readstore.CubletRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.schema.FieldSchema;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processor for Funnel Analysis.
 */
public class FunnelProcessor {
  @Getter
  private final String dataSource;

  private ProjectedTuple tuple;

  @Getter
  private int[] result;

  private String userIdSchema;

  private String actionTimeSchema;

  private final HashSet<String> projectedSchemaSet;

  private final List<BirthSelection> birthSelector;

  /**
   * Constructor.
   *
   * @param layout query for funnel analysis
   */
  public FunnelProcessor(FunnelQuery layout) {
    this.birthSelector = new ArrayList<>();
    for (int i = 0; i < layout.getBirthSelectionLayout().size(); i++) {
      this.birthSelector.add(layout.getBirthSelectionLayout().get(i).generate());
    }
    this.dataSource = layout.getDataSource();
    this.projectedSchemaSet = layout.getFunnelSchemaSet();
    result = new int[this.birthSelector.size()];
    for (int i = 0; i < result.length; i++) {
      result[i] = 0;
    }
  }

  /**
   * Public interface, Scan whole table and return CohortResult.
   *
   * @param cube Cube
   * @return CohortRet
   */
  public int[] process(CubeRS cube) throws IOException {
    // initialize the UserId and KeyId
    TableSchema tableschema = cube.getSchema();
    for (FieldSchema fieldSchema : tableschema.getFields()) {
      if (fieldSchema.getFieldType() == FieldType.UserKey) {
        this.userIdSchema = fieldSchema.getName();
      } else if (fieldSchema.getFieldType() == FieldType.ActionTime) {
        this.actionTimeSchema = fieldSchema.getName();
      }
    }
    // add this two schema into List
    this.projectedSchemaSet.add(this.userIdSchema);
    this.projectedSchemaSet.add(this.actionTimeSchema);
    this.tuple = new ProjectedTuple(this.projectedSchemaSet);
    for (CubletRS cublet : cube.getCublets()) {
      processCublet(cublet);
    }
    return this.result;
  }

  /**
   * Process one Cublet.
   *
   * @param cublet cublet
   */
  private void processCublet(CubletRS cublet) {
    MetaChunkRS metaChunk = cublet.getMetaChunk();
    // transfer values in SetFilter into GloablId
    this.filterInit(metaChunk);

    // use MetaChunk to skip Cublet
    if (!this.checkMetaChunk(metaChunk)) {
      return;
    }

    // Now start to pass the DataChunk
    for (ChunkRS chunk : cublet.getDataChunks()) {
      if (this.checkDataChunk(chunk)) {
        this.processDataChunk(chunk, metaChunk);
      }
    }
  }


  /**
   * In this section, we load the tuple which is an inner property.
   * We left the process logic in processTuple function.
   *
   * @param chunk     dataChunk
   * @param metaChunk metaChunk
   */
  private void processDataChunk(ChunkRS chunk, MetaChunkRS metaChunk) {
    for (int i = 0; i < chunk.getRecords(); i++) {
      // load data into tuple
      for (String schema : this.projectedSchemaSet) {
        int value = chunk.getField(schema).getValueByIndex(i);
        this.tuple.loadAttr(value, schema);
      }
      this.processTuple(metaChunk);
    }
  }

  private void processTuple(MetaChunkRS metaChunk) {
    // For One Tuple, we firstly get the userId, and ActionTime
    int userGlobalID = (int) tuple.getValueBySchema(this.userIdSchema);
    MetaFieldRS userMetaField = metaChunk.getMetaField(this.userIdSchema);
    String userId = userMetaField.getString(userGlobalID);

    LocalDateTime actionTime =
        DateUtils.daysSinceEpoch((int) tuple.getValueBySchema(this.actionTimeSchema));
    // check whether its birthEvent is selected

    for (int i = 0; i < this.birthSelector.size(); i++) {
      if (!this.birthSelector.get(i).isUserSelected(userId)) {
        // if birthEvent is not selected
        this.birthSelector.get(i).selectEvent(userId, actionTime, this.tuple);
        if (this.birthSelector.get(i).isUserSelected(userId)) {
          for (int j = 0; j < i; j++) {
            if (!this.birthSelector.get(j).isUserSelected(userId)) {
              this.birthSelector.get(i).removeUserSelected(userId);
              return;
            }
          }
          String temp = "";
          temp = temp.concat(Integer.toString(i)).concat("     ").concat(userId);
          Logger logger = LoggerFactory.getLogger(FunnelProcessor.class);
          logger.info(temp);
          this.result[i] += 1;
        } else {
          break;
        }
      }
    }
  }

  /**
   * When new Cublet is coming, transfer the set value in Filter to GlobalID.
   *
   * @param metaChunkRS neraChunkRS
   */
  private void filterInit(MetaChunkRS metaChunkRS) {
    // init birthSelector
    for (int i = 0; i < this.birthSelector.size(); i++) {
      for (Filter filter : this.birthSelector.get(i).getFilterList()) {
        filter.loadMetaInfo(metaChunkRS);
      }
    }
  }

  private Boolean checkMetaChunk(MetaChunkRS metaChunk) {

    // 1. check birth selection
    // if the metaChunk contains all birth filter's accept value, then the metaChunk
    // is valid.
    for (int i = 0; i < this.birthSelector.size(); i++) {
      if (this.birthSelector.get(i).getBirthEvents() == null) {
        return true;
      }
    }
    for (int i = 0; i < this.birthSelector.size(); i++) {
      for (Filter filter : this.birthSelector.get(i).getFilterList()) {
        String checkedSchema = filter.getFilterSchema();
        MetaFieldRS metaField = metaChunk.getMetaField(checkedSchema);
        if (this.checkMetaField(metaField, filter)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Now is not implemented.
   */
  public Boolean checkMetaField(MetaFieldRS metaField, Filter ft) {
    return true;
  }

  /***
   * Now is not implemented.
   */
  public Boolean checkDataChunk(ChunkRS chunk) {
    return true;
  }
}