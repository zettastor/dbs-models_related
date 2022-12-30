package py.icshare;

public class RecoverDbSentry {
  private Integer id;

  public RecoverDbSentry() {
  }

  public RecoverDbSentry(Integer id) {
    this.id = id;
  }

  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  @Override
  public String toString() {
    return "RecoverDbSentry{"
        + "id=" + id
        + '}';
  }
}
