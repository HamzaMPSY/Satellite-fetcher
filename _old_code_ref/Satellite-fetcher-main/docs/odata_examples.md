# OData Query Examples for Copernicus

These examples show how to filter Copernicus Sentinel products in OData queries, using collection and productType attributes.

---

## Example: Filter for Sentinel-2, Level-1C, January 2024

```text
$filter=
  Collection/Name eq 'SENTINEL-2'
  and Attributes/OData.CSC.StringAttribute/any(
    att:att/Name eq 'productType' and
    att/OData.CSC.StringAttribute/Value eq 'S2MSI1C'
  )
  and ContentDate/Start gt 2024-01-01T00:00:00.000Z
  and ContentDate/Start lt 2024-01-31T23:59:59.999Z
```
This filters for **Sentinel-2 Level-1C** products (raw scenes, no atmospheric correction/cloud mask) during January 2024.

---

## Attributes/OData Snippet Template

```text
Attributes/OData.CSC.StringAttribute/any(
  att:att/Name eq 'productType' and
  att/OData.CSC.StringAttribute/Value eq '<PRODUCTTYPE>'
)
```
Replace `<PRODUCTTYPE>` with your target product type (see [Copernicus Collections & Product Types](./copernicus_collections.md)).
