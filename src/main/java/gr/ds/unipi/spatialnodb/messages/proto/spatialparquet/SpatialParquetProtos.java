// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: SpatialParquet.proto

package gr.ds.unipi.spatialnodb.messages.proto.spatialparquet;

public final class SpatialParquetProtos {
  private SpatialParquetProtos() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  static com.google.protobuf.Descriptors.Descriptor
    internal_static_Geometry_descriptor;
  static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_Geometry_fieldAccessorTable;
  static com.google.protobuf.Descriptors.Descriptor
    internal_static_Geometry_Part_descriptor;
  static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_Geometry_Part_fieldAccessorTable;
  static com.google.protobuf.Descriptors.Descriptor
    internal_static_Geometry_Part_Coordinate_descriptor;
  static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_Geometry_Part_Coordinate_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\024SpatialParquet.proto\"\225\001\n\010Geometry\022\020\n\010g" +
      "eometry\030\001 \002(\005\022\034\n\004part\030\002 \003(\0132\016.Geometry.P" +
      "art\032Y\n\004Part\022-\n\ncoordinate\030\001 \003(\0132\031.Geomet" +
      "ry.Part.Coordinate\032\"\n\nCoordinate\022\t\n\001x\030\001 " +
      "\002(\001\022\t\n\001y\030\002 \002(\001BA\n\037gr.ds.unipi.spatialnod" +
      "b.parquetB\024SpatialParquetProtosH\001P\001\210\001\001\240\001" +
      "\001"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
      new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
        public com.google.protobuf.ExtensionRegistry assignDescriptors(
            com.google.protobuf.Descriptors.FileDescriptor root) {
          descriptor = root;
          internal_static_Geometry_descriptor =
            getDescriptor().getMessageTypes().get(0);
          internal_static_Geometry_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_Geometry_descriptor,
              new java.lang.String[] { "Geometry", "Part", });
          internal_static_Geometry_Part_descriptor =
            internal_static_Geometry_descriptor.getNestedTypes().get(0);
          internal_static_Geometry_Part_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_Geometry_Part_descriptor,
              new java.lang.String[] { "Coordinate", });
          internal_static_Geometry_Part_Coordinate_descriptor =
            internal_static_Geometry_Part_descriptor.getNestedTypes().get(0);
          internal_static_Geometry_Part_Coordinate_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_Geometry_Part_Coordinate_descriptor,
              new java.lang.String[] { "X", "Y", });
          return null;
        }
      };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
  }

  // @@protoc_insertion_point(outer_class_scope)
}
