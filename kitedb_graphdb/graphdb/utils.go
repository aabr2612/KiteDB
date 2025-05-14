package graphdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// Serialize converts a Node or Edge to a byte slice
func Serialize(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(64) // Pre-allocate for efficiency

	// Write version (1 byte)
	if err := buf.WriteByte(1); err != nil {
		return nil, fmt.Errorf("failed to write version: %v", err)
	}

	switch val := v.(type) {
	case Node:
		// Write ID (8 bytes)
		if err := binary.Write(&buf, binary.LittleEndian, val.ID); err != nil {
			return nil, fmt.Errorf("failed to write node ID: %v", err)
		}
		// Write Active (1 byte)
		if err := binary.Write(&buf, binary.LittleEndian, btoi(val.Active)); err != nil {
			return nil, fmt.Errorf("failed to write node active flag: %v", err)
		}
		// Write number of labels (4 bytes)
		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(val.Labels))); err != nil {
			return nil, fmt.Errorf("failed to write label count: %v", err)
		}
		// Write labels
		for _, label := range val.Labels {
			if err := binary.Write(&buf, binary.LittleEndian, uint32(len(label))); err != nil {
				return nil, fmt.Errorf("failed to write label length for %q: %v", label, err)
			}
			if _, err := buf.WriteString(label); err != nil {
				return nil, fmt.Errorf("failed to write label %q: %v", label, err)
			}
		}
		// Write number of properties (4 bytes)
		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(val.Properties))); err != nil {
			return nil, fmt.Errorf("failed to write property count: %v", err)
		}
		// Write properties
		for _, prop := range val.Properties {
			if err := writeProperty(&buf, prop); err != nil {
				return nil, fmt.Errorf("failed to serialize property %q: %v", prop.Key, err)
			}
		}
	case Edge:
		// Write ID (8 bytes)
		if err := binary.Write(&buf, binary.LittleEndian, val.ID); err != nil {
			return nil, fmt.Errorf("failed to write edge ID: %v", err)
		}
		// Write Active (1 byte)
		if err := binary.Write(&buf, binary.LittleEndian, btoi(val.Active)); err != nil {
			return nil, fmt.Errorf("failed to write edge active flag: %v", err)
		}
		// Write Type length and value
		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(val.Type))); err != nil {
			return nil, fmt.Errorf("failed to write type length: %v", err)
		}
		if _, err := buf.WriteString(val.Type); err != nil {
			return nil, fmt.Errorf("failed to write edge type %q: %v", val.Type, err)
		}
		// Write Source and Target (8 bytes each)
		if err := binary.Write(&buf, binary.LittleEndian, val.Source); err != nil {
			return nil, fmt.Errorf("failed to write source ID: %v", err)
		}
		if err := binary.Write(&buf, binary.LittleEndian, val.Target); err != nil {
			return nil, fmt.Errorf("failed to write target ID: %v", err)
		}
		// Write number of properties (4 bytes)
		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(val.Properties))); err != nil {
			return nil, fmt.Errorf("failed to write property count: %v", err)
		}
		// Write properties
		for _, prop := range val.Properties {
			if err := writeProperty(&buf, prop); err != nil {
				return nil, fmt.Errorf("failed to serialize property %q: %v", prop.Key, err)
			}
		}
	default:
		return nil, fmt.Errorf("unsupported type for serialization: %T", v)
	}
	return buf.Bytes(), nil
}

// Deserialize converts a byte slice to a Node or Edge
func Deserialize(data []byte, v interface{}) error {
	buf := bytes.NewReader(data)
	if len(data) == 0 {
		return fmt.Errorf("empty data for deserialization")
	}

	// Read version
	var version byte
	if err := binary.Read(buf, binary.LittleEndian, &version); err != nil {
		return fmt.Errorf("failed to read version: %v", err)
	}
	if version != 1 {
		return fmt.Errorf("unsupported version: %d", version)
	}

	switch val := v.(type) {
	case *Node:
		// Read ID
		var id int64
		if err := binary.Read(buf, binary.LittleEndian, &id); err != nil {
			return fmt.Errorf("failed to read node ID: %v", err)
		}
		val.ID = id
		// Read Active
		var activeInt byte
		if err := binary.Read(buf, binary.LittleEndian, &activeInt); err != nil {
			return fmt.Errorf("failed to read node active flag: %v", err)
		}
		val.Active = activeInt != 0
		// Read number of labels
		var labelCount uint32
		if err := binary.Read(buf, binary.LittleEndian, &labelCount); err != nil {
			return fmt.Errorf("failed to read label count: %v", err)
		}
		// Read labels
		val.Labels = make([]string, labelCount)
		for i := uint32(0); i < labelCount; i++ {
			var lenLabel uint32
			if err := binary.Read(buf, binary.LittleEndian, &lenLabel); err != nil {
				return fmt.Errorf("failed to read label length at index %d: %v", i, err)
			}
			if int(lenLabel) > buf.Len() {
				return fmt.Errorf("label length %d exceeds remaining buffer %d", lenLabel, buf.Len())
			}
			labelBytes := make([]byte, lenLabel)
			if _, err := buf.Read(labelBytes); err != nil {
				return fmt.Errorf("failed to read label at index %d: %v", i, err)
			}
			val.Labels[i] = string(labelBytes)
		}
		// Read number of properties
		var propCount uint32
		if err := binary.Read(buf, binary.LittleEndian, &propCount); err != nil {
			return fmt.Errorf("failed to read property count: %v", err)
		}
		// Read properties
		val.Properties = make([]Property, propCount)
		for i := uint32(0); i < propCount; i++ {
			if err := readProperty(buf, &val.Properties[i]); err != nil {
				return fmt.Errorf("failed to deserialize property at index %d: %v", i, err)
			}
		}
	case *Edge:
		// Read ID
		var id int64
		if err := binary.Read(buf, binary.LittleEndian, &id); err != nil {
			return fmt.Errorf("failed to read edge ID: %v", err)
		}
		val.ID = id
		// Read Active
		var activeInt byte
		if err := binary.Read(buf, binary.LittleEndian, &activeInt); err != nil {
			return fmt.Errorf("failed to read edge active flag: %v", err)
		}
		val.Active = activeInt != 0
		// Read Type
		var lenType uint32
		if err := binary.Read(buf, binary.LittleEndian, &lenType); err != nil {
			return fmt.Errorf("failed to read type length: %v", err)
		}
		if int(lenType) > buf.Len() {
			return fmt.Errorf("type length %d exceeds remaining buffer %d", lenType, buf.Len())
		}
		typeBytes := make([]byte, lenType)
		if _, err := buf.Read(typeBytes); err != nil {
			return fmt.Errorf("failed to read edge type: %v", err)
		}
		val.Type = string(typeBytes)
		// Read Source and Target
		var source, target int64
		if err := binary.Read(buf, binary.LittleEndian, &source); err != nil {
			return fmt.Errorf("failed to read source ID: %v", err)
		}
		if err := binary.Read(buf, binary.LittleEndian, &target); err != nil {
			return fmt.Errorf("failed to read target ID: %v", err)
		}
		val.Source = source
		val.Target = target
		// Read number of properties
		var propCount uint32
		if err := binary.Read(buf, binary.LittleEndian, &propCount); err != nil {
			return fmt.Errorf("failed to read property count: %v", err)
		}
		// Read properties
		val.Properties = make([]Property, propCount)
		for i := uint32(0); i < propCount; i++ {
			if err := readProperty(buf, &val.Properties[i]); err != nil {
				return fmt.Errorf("failed to deserialize property at index %d: %v", i, err)
			}
		}
	default:
		return fmt.Errorf("unsupported type for deserialization: %T", v)
	}
	return nil
}

// writeProperty serializes a single property
func writeProperty(buf *bytes.Buffer, prop Property) error {
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(prop.Key))); err != nil {
		return fmt.Errorf("failed to write key length: %v", err)
	}
	if _, err := buf.WriteString(prop.Key); err != nil {
		return fmt.Errorf("failed to write key: %v", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, byte(prop.Type)); err != nil {
		return fmt.Errorf("failed to write type: %v", err)
	}
	switch prop.Type {
	case PropertyInt:
		if v, ok := prop.Value.(int64); ok {
			if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
				return fmt.Errorf("failed to write int64 value: %v", err)
			}
		} else {
			return fmt.Errorf("invalid int64 value for property %q: %T", prop.Key, prop.Value)
		}
	case PropertyString:
		if v, ok := prop.Value.(string); ok {
			if err := binary.Write(buf, binary.LittleEndian, uint32(len(v))); err != nil {
				return fmt.Errorf("failed to write string length: %v", err)
			}
			if _, err := buf.WriteString(v); err != nil {
				return fmt.Errorf("failed to write string value: %v", err)
			}
		} else {
			return fmt.Errorf("invalid string value for property %q: %T", prop.Key, prop.Value)
		}
	case PropertyBool:
		if v, ok := prop.Value.(bool); ok {
			if err := binary.Write(buf, binary.LittleEndian, btoi(v)); err != nil {
				return fmt.Errorf("failed to write bool value: %v", err)
			}
		} else {
			return fmt.Errorf("invalid bool value for property %q: %T", prop.Key, prop.Value)
		}
	default:
		return fmt.Errorf("unsupported property type %d for property %q", prop.Type, prop.Key)
	}
	return nil
}

// readProperty deserializes a single property
func readProperty(buf *bytes.Reader, prop *Property) error {
	var lenKey uint32
	if err := binary.Read(buf, binary.LittleEndian, &lenKey); err != nil {
		return fmt.Errorf("failed to read key length: %v", err)
	}
	if int(lenKey) > buf.Len() {
		return fmt.Errorf("key length %d exceeds remaining buffer %d", lenKey, buf.Len())
	}
	keyBytes := make([]byte, lenKey)
	if _, err := buf.Read(keyBytes); err != nil {
		return fmt.Errorf("failed to read key: %v", err)
	}

	var propType byte
	if err := binary.Read(buf, binary.LittleEndian, &propType); err != nil {
		return fmt.Errorf("failed to read property type: %v", err)
	}
	prop.Type = PropertyType(propType)

	var value interface{}
	switch prop.Type {
	case PropertyInt:
		var v int64
		if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
			return fmt.Errorf("failed to read int64 value: %v", err)
		}
		value = v
	case PropertyString:
		var lenValue uint32
		if err := binary.Read(buf, binary.LittleEndian, &lenValue); err != nil {
			return fmt.Errorf("failed to read string length: %v", err)
		}
		if int(lenValue) > buf.Len() {
			return fmt.Errorf("string length %d exceeds remaining buffer %d", lenValue, buf.Len())
		}
		valueBytes := make([]byte, lenValue)
		if _, err := buf.Read(valueBytes); err != nil {
			return fmt.Errorf("failed to read string value: %v", err)
		}
		value = string(valueBytes)
	case PropertyBool:
		var v byte
		if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
			return fmt.Errorf("failed to read bool value: %v", err)
		}
		value = v != 0
	default:
		return fmt.Errorf("unsupported property type %d", prop.Type)
	}
	prop.Key = string(keyBytes)
	prop.Value = value
	return nil
}

// btoi converts bool to byte (0 or 1)
func btoi(b bool) byte {
	if b {
		return 1
	}
	return 0
}
